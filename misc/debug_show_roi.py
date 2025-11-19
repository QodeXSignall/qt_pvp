import cv2
from movement_gps_time_analyzer import AnalyzerConfig, ROI

video = "input.mp4"

config = AnalyzerConfig()

# вот здесь можно вручную менять координаты:
config.motion_roi = ROI(55, 30, 580, 60)

cap = cv2.VideoCapture(video)
while True:
    ret, frame = cap.read()
    if not ret:
        break

    # рисуем ROI
    roi = config.motion_roi
    cv2.rectangle(frame, (roi.x1, roi.y1), (roi.x2, roi.y2), (0, 255, 0), 2)

    cv2.imshow("ROI debugger", frame)
    if cv2.waitKey(30) & 0xFF == ord("q"):
        break

cap.release()
cv2.destroyAllWindows()
