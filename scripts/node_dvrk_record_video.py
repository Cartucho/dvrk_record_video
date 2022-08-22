#!/usr/bin/env python3
import sys
#print(sys.version)
import os
import glob
import time
import yaml

import rospkg
import rospy
import message_filters
from sensor_msgs.msg import Image
from cv_bridge import CvBridge, CvBridgeError

import cv2 as cv
import numpy as np
import subprocess as sp

import multiprocessing as mp
import signal


class Sync:
  def __init__(self, path_package, config):
    """ Load settings from config file """
    self.is_visualization_on = config["show_visualization"]
    self.save_frames_timestamp = config["save_frames_timestamp"]
    self.discard_individual_frames = config["vid"]["discard_individual_frames"]
    self.fps = float(config["vid"]["fps"])
    self.is_otf_compression_on = config["on_the_fly_lossless_compression"]["is_on"]
    self.is_vid_on = config["vid"]["is_on"]
    self.frame_pad = "07d"
    self.frame_frmt = ".bmp" # To keep it lossless and since `.bmp` is the fastest to write
    if self.is_otf_compression_on:
      self.set_up_otf_compression(config["on_the_fly_lossless_compression"]["format_opti"],
                                  config["on_the_fly_lossless_compression"]["n_cores"],
                                  config["on_the_fly_lossless_compression"]["counter_between_runs"])
    self.set_up_output_dir(path_package, config["output_dir"])
    self.set_up_output_file_paths(config["vid"]["format"])
    if self.is_vid_on:
      self.set_up_ffmpeg_command(config["vid"]["codec"], config["vid"]["crf"])
    """ Initialize flags for processing """
    self.is_shutdown = False
    self.is_processing = False
    self.is_new_msg = False
    self.counter_save = 0
    """ Subscribe to camera topics """
    self.data_sub_im1 = message_filters.Subscriber(config["rostopic"]["cam1"], Image)
    self.data_sub_im2 = message_filters.Subscriber(config["rostopic"]["cam2"], Image)
    self.ats = message_filters.ApproximateTimeSynchronizer([self.data_sub_im1,
                                                            self.data_sub_im2],
                                                            queue_size=500,
                                                            slop=1./self.fps)
    self.bridge = CvBridge()
    self.initialize_recording()
    self.ats.registerCallback(self.sync_callback)


  def split_list(self, l, n):
    k, m = divmod(len(l), n)
    return list(l[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))


  def set_up_otf_compression(self, frmt_opti, n_cores_requested, counter_between_runs):
    """ On-the-fly compression `frame_frmt` -> `frame_frmt_opti` """
    self.frame_frmt_opti = frmt_opti # Convert `.bmp` to `frmt_opti` in separate processes, to save memory
    """ I will split the frames between processes according to their name's ending, e.g. %0.bpm, %1.bpm """
    ends_all = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    n_cores = min(n_cores_requested, 10) # Max we need 10 cores, since there are 10 numbers in total
    n_cores_max = mp.cpu_count()
    self.n_cores = min(n_cores, n_cores_max) # We cannot request more cores than what the computer is equipped with
    self.counter_between_runs = counter_between_runs
    self.ends_splitted = self.split_list(ends_all, self.n_cores)
    self.counter_compress = 0
    self.frame_compress_completed = True
    self.pool = mp.Pool(self.n_cores)


  def set_up_output_dir(self, path_package, out_dir):
    if not os.path.isabs(out_dir):
      out_dir = os.path.join(path_package, out_dir)
    if not os.path.isdir(out_dir):
      os.makedirs(out_dir)
      rospy.loginfo("Directory {} created".format(out_dir))
    self.out_dir = out_dir


  def set_up_ffmpeg_command(self, codec, crf):
    """ This is the command that will be run at the end of the frame collection, to compress the frames into a video """
    frame_frmt = self.frame_frmt
    if self.is_otf_compression_on:
      frame_frmt = self.frame_frmt_opti
    self.ffmpeg_command = ["ffmpeg",
                           "-r", "{}".format(self.fps),                                   # Frames per second
                           "-i", "{}_%{}{}".format(self.out, self.frame_pad, frame_frmt), # Input images
                           "-c:v", "{}".format(codec),                                    # Video codec
                           "-b:v", "0",                                                   # Set bitrate to 0, to adjust quality from the "crf" value
                           "-crf", "{}".format(crf),                                      # Constant quality encoding (the lower the better the quality)
                           "-strict", "experimental",                                     # To allow experimental codecs as well
                           "-an",                                                         # No audio
                           "{}".format(self.out_vid)]                                     # Output video path


  def set_up_output_file_paths(self, out_vid_format):
    self.timestr = time.strftime("%Y-%m-%d_%H-%M-%S")
    self.out = os.path.join(self.out_dir, "{}".format(self.timestr))
    if self.save_frames_timestamp:
        self.out_stamp = "{}_timestamps.txt".format(self.out)
    self.out_vid = "{}{}".format(self.out, out_vid_format)


  def initialize_recording(self):
    rospy.loginfo("Recording started!")
    if self.save_frames_timestamp:
      self.f = open(self.out_stamp, "a")


  def sync_callback(self, msg_im1, msg_im2):
    if self.is_shutdown:
      # The callback may still be called when compressing the video at the end
      return
    try:
      im1 = self.bridge.imgmsg_to_cv2(msg_im1, "bgr8")
      im2 = self.bridge.imgmsg_to_cv2(msg_im2, "bgr8")
    except CvBridgeError as e:
      print(e)
      return
    self.frame = np.hstack((im1, im2))
    frame_timestamp = msg_im1.header.stamp
    """ Save image """
    frame_path = "{}_{:{}}{}".format(self.out, self.counter_save, self.frame_pad, self.frame_frmt)
    cv.imwrite(frame_path, self.frame)
    self.counter_save += 1
    if self.save_frames_timestamp:
      """ Save image timestamp """
      self.f.write('{}\n'.format(frame_timestamp))
    """ On-the-fly frame compression """
    if self.is_otf_compression_on:
      if self.counter_save % self.counter_between_runs == 0:
        if self.frame_compress_completed:
          self.start_processes_to_compress_otf()


  def log_result(self, n_files):
    self.counter_compress += n_files
    if self.counter_compress == self.counter_compress_goal:
      self.frame_compress_completed = True


  def start_processes_to_compress_otf(self):
    self.frame_compress_completed = False
    self.counter_compress_goal = self.counter_save
    for ends in self.ends_splitted:
      frame_path = "{}*{}{}".format(self.out, ends, self.frame_frmt)
      self.pool.apply_async(compress_frames, args=(frame_path, self.frame_frmt, self.frame_frmt_opti), callback=self.log_result)


  def stop_recording_and_compress_video(self):
    self.is_shutdown = True
    rospy.loginfo("Stopped recording new frames!")
    if self.is_visualization_on:
      cv.destroyAllWindows()
    time.sleep(2)
    if self.save_frames_timestamp:
      if not self.f.closed:
        self.f.close() # Close file with timestamps
    frame_frmt = self.frame_frmt
    if self.is_otf_compression_on:
      rospy.loginfo("Please wait until all the individual frames are compressed...")
      while not self.frame_compress_completed:
        time.sleep(3)
      rospy.loginfo("Running the last compression of frames... It may take a while, please wait")
      self.start_processes_to_compress_otf()
      self.pool.close()
      self.pool.join()
      frame_frmt = self.frame_frmt_opti
    frame_path = "{}*{}".format(self.out, frame_frmt)
    path_im_list = glob.glob(frame_path)
    if path_im_list:
      rospy.loginfo("Compressing recorded images into video. Please wait...")
      if self.is_vid_on:
        self.process = sp.Popen(self.ffmpeg_command, stdin=sp.PIPE)
        self.process.wait() # Wait for sub-process to finish
        if self.discard_individual_frames:
          rospy.loginfo("Discarding individual frames...")
          for path_im in path_im_list:
            os.remove(path_im)#os.path.join(self.out_dir, path_im))
    rospy.loginfo("All done!")


def get_config_data(path_package):
  path_config = os.path.join(path_package, 'config.yaml')
  with open(path_config, 'r') as stream:
    try:
        config = yaml.safe_load(stream)
        return config
    except yaml.YAMLError as e:
        print(e)


def compress_frames(frame_path, frame_frmt, frame_frmt_opti):
  signal.signal(signal.SIGINT, signal.SIG_IGN) # Make it ignore Ctrl + C, not to interfere with ROS Ctrl + C
  files = glob.glob(frame_path)
  if files:
    for file in files:
      frame = cv.imread(file)
      cv.imwrite(file.replace(frame_frmt, frame_frmt_opti), frame)
      os.remove(file)
  return len(files)


def main(args):
  rospy.loginfo("Initializing node...")
  rospy.init_node("node_dvrk_record_video", anonymous=True)
  rospack = rospkg.RosPack()
  name_package = "dvrk_record_video"
  path_package = rospack.get_path(name_package)
  config = get_config_data(path_package)
  sync = Sync(path_package, config)
  rospy.on_shutdown(sync.stop_recording_and_compress_video)
  rospy.loginfo("Recording individual frames, and then compress when finished recording...")
  r = rospy.Rate(sync.fps)
  if sync.is_visualization_on:
    cv.namedWindow(name_package, cv.WINDOW_KEEPRATIO)
  while not rospy.is_shutdown():
    """ Show what is being recorded """
    if sync.is_visualization_on:
      if sync.frame is not None:
        cv.imshow(name_package, sync.frame)
        pressed_key = cv.waitKey(1)
        if pressed_key == ord('q'): # User pressed 'q' to quit
          rospy.signal_shutdown("User requested to quit")
    r.sleep() # Keep loop running at the camera's FPS, by accounting the time used by any operations during the loop


if __name__ == '__main__':
    main(sys.argv)
