ffmpeg -rtsp_transport tcp -i $1 -vcodec copy -map 0 -f segment -segment_time 300 -segment_format mp4 "ffmpeg_capture-%03d.mp4"