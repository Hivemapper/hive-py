import numpy as np
import cv2
import random
import math
import statistics
import os
import argparse
import zipfile
from exiftool import ExifToolHelper
from datetime import datetime
import time



def optical_flow(image_files: list[list[str]], max_corners: int, num_random_checks: int, threshold_dxdy_ratio: float):
    """ For camera orientation classification using optical flow.
    Args:
        image_files: list of groups of images. Each group is a list of image file paths representing a continuous drive segment.
        max_corners: Max number of features to track for optical flow
        num_random_checks: Number of random checks to perform
        threshold_dxdy_ratio: threshold for classifying camera orientation
    """
    # Check if there are enough frames
    total_frames = len([item for sublist in image_files for item in sublist])
    if (total_frames > 1):

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

        # Check optical flow at random locations
        Dx = []
        Dy = []

        for i in range(0, min(num_random_checks, total_frames)):
            # Randomly select group of images
            random_group = math.floor((random.uniform(0, 1))*(len(image_files)-1))
            # Randomly select a frame, ensuring that the last frame is not selected
            rand_frame = math.floor((random.uniform(0, 1))*(len(image_files[random_group])-2))

            # Get first frame
            frame1 = cv2.imread(image_files[random_group][rand_frame])
            gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY)

            # Get features to track
            p0 = cv2.goodFeaturesToTrack(
                gray1, mask=None, **feature_params)

            # Get second frame
            frame2 = cv2.imread(image_files[random_group][rand_frame+1])
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
            if (len(good_new) <= 0):
                print("No features found in frame ", image_files[i])
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
                print("Right side")
            else:
                print("Left side")
        else:
            print("Front or back")
    else:
        print("Less than 1 frames. Skipping optical flow based camera mount classification.")


def list_image_files(directory: str, unzip=False):
    """
    List and sorts all JPEG image files in a directory.

    If 'unzip' is True and the directory is a zip file, it extracts the contents.

    Parameters:
        directory (str): Path to the directory or zip file.
        unzip (bool, optional): Extract zip file if True. Defaults to False.

    Returns:
        list: List of JPEG image file paths.

    Example:
        >>> list_image_files("/path/to/images", unzip=True)
        ['/path/to/images/image1.jpg', '/path/to/images/image2.jpg', ...]
    """
    image_files = []
    if unzip and directory.endswith(".zip"):
        with zipfile.ZipFile(directory, 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(directory))
        directory = os.path.splitext(directory)[0]  # Update directory to the extracted folder
    for filename in os.listdir(directory):
        if filename.endswith(".jpg") or filename.endswith(".jpeg"):
            image_files.append(os.path.join(directory, filename))
    try:
        return sorted(image_files, key=lambda x: int(x.split('/')[-1].split('.')[0]))
    except:
        return sorted(image_files)


def extract_coordinates(image_path: str):
    """ 
    Extract GPS and DateTimeOriginal data from an image.

    Parameters:
        image_path (str): Path to the image file.
    
    Returns:
        dict: Dictionary containing GPS and DateTimeOriginal data.

    Example:
        >>> extract_coordinates("/path/to/image.jpg")
        {
            "GPSLatitudeRef": "N",
            "GPSLatitude": "37 deg 48' 36.00\" N",
            "GPSLongitudeRef": "W",
            "GPSLongitude": "122 deg 16' 30.00\" W",
            "GPSAltitude": "0 m",
            "DateTimeOriginal": 1620000000,
        }

    """
    with ExifToolHelper() as et:
        metadata_list = et.get_metadata(image_path)
        # Initialize a dictionary to hold the GPS values and DateTimeOriginal
        gps_data = {
            "GPSLatitudeRef": None,
            "GPSLatitude": None,
            "GPSLongitudeRef": None,
            "GPSLongitude": None,
            "GPSAltitude": None,
            "DateTimeOriginal": None,
        }
        
        # Loop through each metadata dictionary
        for metadata in metadata_list:
            # Check and extract the GPS metadata and DateTimeOriginal
            for key in list(gps_data.keys()): 
                exif_key = f"EXIF:{key}"
                if exif_key in metadata:
                    if key == "DateTimeOriginal":
                        # Convert DateTimeOriginal to epoch time
                        date_time_obj = datetime.strptime(metadata[exif_key], '%Y:%m:%d %H:%M:%S')
                        gps_data[key] = int(time.mktime(date_time_obj.timetuple()))
                    else:
                        # Save the value with the simplified key for GPS data
                        gps_data[key] = metadata[exif_key]
        
        return gps_data

def extract_all_path_data(image_files: list[str]):
    """
    Extract GPS and DateTimeOriginal data from all images in a list.

    Parameters:
        image_files (list): List of image file paths.

    Returns:
        list: List of dictionaries containing GPS and DateTimeOriginal data for each image.

    Example:
        >>> extract_all_path_data(["/path/to/image1.jpg", "/path/to/image2.jpg", ...])
        [
            {
                "GPSLatitudeRef": "N",
                "GPSLatitude": "37 deg 48' 36.00\" N",
                "GPSLongitudeRef": "W",
                "GPSLongitude": "122 deg 16' 30.00\" W",
                "GPSAltitude": "0 m",
                "DateTimeOriginal": 1620000000,
            },
            ...
        ]
    """
    return [extract_coordinates(image) for image in image_files]

def extract_values(dicts: list[dict], key: str):
    """
    Extracts values from a list of dictionaries based on a specified key.

    Parameters:
    - dicts (list of dict): A list of dictionaries from which to extract the values.
    - key (str): The key whose values are to be extracted from the dictionaries.

    Returns:
    - list: A list of values corresponding to the specified key from each dictionary.
            If a dictionary does not have the key, `None` is included in the list.
    """
    return [d.get(key, None) for d in dicts]

def calculate_headings(xs: list[float], ys: list[float]):
    """
    Calculates headings based on x and y coordinates. The list of headings
    will match the length of the coordinates list by repeating the last heading
    for the final coordinate.

    Parameters:
    - xs (list of float): X coordinates.
    - ys (list of float): Y coordinates.

    Returns:
    - list of float: Calculated headings in radians for each coordinate.
    """
    headings = []

    for i in range(1, len(xs)):
        dx = xs[i] - xs[i-1]
        dy = ys[i] - ys[i-1]
        heading = math.atan2(dy, dx)
        headings.append(heading)
    
    # Repeat the last heading for the final coordinate
    headings.append(headings[-1])

    return headings

def make_headings_continuous(headings):
    """
    Adjusts an array of headings to be continuous by ensuring that each heading
    change does not exceed ±π radians from the previous heading.

    Parameters:
    - headings (list of float): An array of headings in radians.

    Returns:
    - list of float: A new array of adjusted, continuous headings.
    """
    if not headings:
        return [] 

    corrected_headings = [headings[0]] 
    two_pi = 2 * math.pi
    for i in range(1, len(headings)):
        diff = headings[i] - headings[i - 1]

        # Normalize the difference to be within the range [-π, π]
        if diff > math.pi:
            diff -= two_pi
        elif diff < -math.pi:
            diff += two_pi

        # Adjust the current heading based on the normalized difference
        corrected_headings.append(corrected_headings[i - 1] + diff)

    return corrected_headings

def find_stable_headings(headings, threshold):
    """
    Find indexes of headings where changes do not exceed a set threshold, assuming
    headings are continuous and don't require wrap-around handling.

    Parameters:
    - headings (list of float): List of continuous heading values in radians.
    - threshold (float): The maximum allowed change in heading to be considered stable in radians.

    Returns:
    - list of int: Indexes of the first heading in each pair considered stable.
    """
    stable_indexes = []
    for i in range(len(headings) - 1):
        # Calculate the absolute difference in heading
        diff = abs(headings[i] - headings[i + 1])

        if diff <= threshold:
            stable_indexes.append(i)
    
    return stable_indexes

def group_consecutive_and_filter_out_small_groups(indexes):
    """
    Groups consecutive integers in a list and drops groups with fewer than two items.

    Parameters:
    - indexes (list): List of integers.

    Returns:
    - A list of lists, each containing consecutive integers, with groups of 2 or less dropped.
    """
    if not indexes:
        return []

    # Sort indexes to ensure correct ordering
    sorted_indexes = sorted(indexes)
    grouped = [[sorted_indexes[0]]]  

    for index in sorted_indexes[1:]:
        if index == grouped[-1][-1] + 1:
            # If current index is consecutive, add it to the last group
            grouped[-1].append(index)
        else:
            # Otherwise, start a new group
            grouped.append([index])

    # Filter out groups with fewer than two items
    filtered_grouped = [group for group in grouped if len(group) >= 2]

    return filtered_grouped


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for camera orientation classification using optical flow.")
    parser.add_argument("image_files_directory", help="Path to the directory of a singluar drive of image files")
    parser.add_argument("--unzip", action="store_true", help="Unzip the input directory if it's a zip file")
    parser.add_argument("--max_corners", type=int, help="Max number of features to track for optical flow", default=300)
    parser.add_argument("--num_random_checks", type=int, help="Number of random checks", default=10)
    parser.add_argument("--threshold_dxdy_ratio", type=float, help="Threshold for classifying camera orientation", default=3.0)
    parser.add_argument("--turn_threshold", type=float, help="Threshold for difference in radians from one frame to the next to consider the vehicle turning", default=0.07)
    args = parser.parse_args()


    print("Extracting gnss coordinates from images...")
    files = list_image_files(args.image_files_directory, args.unzip)
    extracted_path_data = extract_all_path_data(files)
    latitudes = extract_values(extracted_path_data, "GPSLatitude")
    longitudes = extract_values(extracted_path_data, "GPSLongitude")
    print("Identifying and removing turns during drive data...")
    headings = calculate_headings(latitudes, longitudes)
    continuous_headings = make_headings_continuous(headings)
    stable_indexes = group_consecutive_and_filter_out_small_groups(find_stable_headings(continuous_headings, args.turn_threshold))
    # Grab groups of images that are not turns
    filtered_list = []
    count = 0
    for group in stable_indexes:
        sub_list = []
        for i in group:
            sub_list.append(files[i])
            count += 1
        filtered_list.append(sub_list)
    print(F"Total frames received: {len(files)}")
    print(F"Total frames after filtering out turns: {count}")
    print("Calculating optical flow...")
    optical_flow(filtered_list, args.max_corners, args.num_random_checks, args.threshold_dxdy_ratio)
    