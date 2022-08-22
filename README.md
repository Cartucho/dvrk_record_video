# dvrk_record_video

A ROS package for recording stereo video.

# How it works?

The messages from the stereo cameras are being synchronized and converted into individual frames.
Each frame is composed of two images, the left stereo image on the left, and the right stereo image on the right.
The frames are stored in `.bmp` format, to save the image as fast as possible.

If desired, the `.bmp` images will be converted on-the-fly into a more memory efficient format like `.webp`.
This on-the-fly conversion is ran in parallel by different processes running on separate cores, to avoid interfering with the real-time frame recording.

At the end of the recording, when the user presses `Ctrl + C`, the frames are compressed into a video.
The quality of the video, lossy versus lossless, and many other settings can be adjusted to your preference.

Please read carefully the [config.yaml](config.yaml) file and ajust the settings as desired.
