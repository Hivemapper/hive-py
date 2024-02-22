import numpy as np
import cv2
import random
import math
import statistics
import os
import argparse


def main(image_files: list[str], max_corners: int, num_random_checks: int, threshold_dxdy_ratio: float):
    """ For camera orientation classification using optical flow.
    Args:
        image_files: list of image file paths
        max_corners: Max number of features to track for optical flow
        num_random_checks: Number of spots in the video to check when detemining camera orientation
        threshold_dxdy_ratio: threshold for classifying camera orientation
    """
    # Ensure that the number of frames is greater than 1
    if (len(image_files) > 1):
        print("Checking for camera mount classification using optical flow.")

        # Count the number of frames
        total_frames =len(image_files)

        # Set parameters for corner detection
        feature_params = dict(maxCorners=max_corners,
                            qualityLevel=0.1,
                            minDistance=7,
                            blockSize=7)

        # Set parameters for lucas kanade optical flow
        lk_params = dict(winSize=(15, 15),
                        maxLevel=6,
                        criteria=(cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT,
                                10, 0.03))

        # Create some random colors
        color = np.random.randint(0, 255, (max_corners, 3))

        # Check optical flow at random locations
        Dx = []
        Dy = []

        for i in range(0, num_random_checks):

            # Get first frame
            rand_frame = math.floor((random.uniform(0, 1))*(total_frames-1))
            frame1 = cv2.imread(image_files[rand_frame])
            gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY)

            # Create a mask image for drawing purposes
            mask = np.zeros_like(frame1)

            # Get features to track
            p0 = cv2.goodFeaturesToTrack(
                gray1, mask=None, **feature_params)

            # Get second frame
            frame2 = cv2.imread(image_files[rand_frame+1])
            gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY)

            # Calculate optical flow
            p1, st, err = cv2.calcOpticalFlowPyrLK(
                gray1, gray2, p0, None, **lk_params)

            # Select good points
            good_new = p1[st == 1]
            good_old = p0[st == 1]

            # Draw the tracks
            dy = 0
            dx = 0
            for i, (new, old) in enumerate(zip(good_new,
                                            good_old)):
                a, b = new.ravel()
                c, d = old.ravel()
                dx = dx + c-a
                dy = dy + d-b

            if (len(good_new) == 0):
                print("No features found in frame " + str(rand_frame))
                continue
            Dx.append(dx/len(good_new))
            Dy.append(dy/len(good_new))

        # Do classification
        DxDyRatios = []
        for i in range(0, len(Dx)):
            DxDyRatios.append(abs(Dx[i]/Dy[i]))

        # Check for silent failure
        if (len(DxDyRatios) == 0):
            print("Camera mount checking failed.")
            exit

        # Classify camera mount
        if (statistics.median(DxDyRatios) > threshold_dxdy_ratio):
            if (statistics.median(Dx) < 0.0):
                print("Passenger side camera orientation")
            else:
                print("Driver side camera orientation")
        else:
            print("Forward or backward facing camera orientation")
        print("FINISHED!")
    else:
        print("Less than 2 frames extracted. Skipping optical flow based camera mount classificatin.")
        print("FINISHED!")

def list_jpg_files(directory: str):
    """Reads a directory and returns a list of all jpg files in the directory.
    Args:
        directory (string): Path to the directory
    Returns:
        list: List of all jpg files in the directory
    """
    jpg_files = []
    for filename in os.listdir(directory):
        if filename.endswith(".jpg"):
            jpg_files.append(os.path.join(directory, filename))
    return jpg_files

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for camera orientation classification using optical flow.")
    parser.add_argument("image_files_directory", help="Path to the directory of a singluar drive of image files")
    parser.add_argument("--max_corners", type=int, help="Max number of features to track for optical flow", default=50)
    parser.add_argument("--num_random_checks", type=int, help="Number of random checks", default=20)
    parser.add_argument("--threshold_dxdy_ratio", type=float, help="Threshold for classifying camera orientation", default=2.0)
    args = parser.parse_args()

    main(list_jpg_files(args.image_files_directory), args.max_corners, args.num_random_checks, args.threshold_dxdy_ratio)