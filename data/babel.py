import os
import wget
import tarfile
import argparse
import csv
from multiprocessing.pool import ThreadPool
import subprocess
from utils import create_manifest
import re
from sphfile import SPHFile

parser = argparse.ArgumentParser(description='Processes downloaded IARPA babel corpus')
parser.add_argument("--target-dir", default='CommonVoice_dataset/', type=str, help="Directory to store the dataset.")
parser.add_argument("--data-dir", type=str, help="Path to the BABEL directory file ")
parser.add_argument('--sample-rate', default=16000, type=int, help='Sample rate')
parser.add_argument('--min-duration', default=1, type=int,
                    help='Prunes training samples shorter than the min duration (given in seconds, default 1)')
parser.add_argument('--max-duration', default=15, type=int,
                    help='Prunes training samples longer than the max duration (given in seconds, default 15)')
args = parser.parse_args()

def read_transcription_file(file_path, audio_file_path):
    """Read transcription files from the IARPA babel format.
    
    Transcription files consist of the following format
    [timestamp]
    transcription
    [timestamp]
    transcription
    [timestamp]
    
    Args:
        file_path: str, path to the transcription file to read.
        audio_file_path: str, path to the sph file that corresponds to
        the given transcription file.
    Returns:
        an array of dicts where the following keys are used: 
            'start_time', 'end_time', 'transcription', 'audio_file'
    """
    with open(file_path) as in_file:
        last_timestamp = 0
        res = []
        transcription = ""
        for line in in_file:
            time_stamp_match = re.match("\[([0-9\]+\.[0-9]+)\]", line)
            #if this regex matched then the line is a timestamp
            if time_stamp_match:
                timestamp = float(time_stamp_match.group(1))
                if transcription and transcription.strip() not in ['(())',  "<no-speech>"]:
                    single_instance = {"start_time": last_timestamp, 
                                       "end_time": timestamp,
                                       "transcription": transcription,
                                       "audio_file" : audio_file_path}
                    res.append(single_instance)
                    last_timestamp = timestamp
                else:
                    last_timestamp = timestamp # this handles silence at beginning
            else:
                transcription = line.strip()
        
        return res

def convert_to_wav(txt_file, sph_path, target_dir):
    """ Read *.csv file description, convert mp3 to wav, process text.
        Save results to target_dir.

    Args:
        txt_file: str, path to *.txt file with data description, 
                  usually contained in the transcription folder
        target_dir: str, path to dir to save results; wav/ and txt/ dirs will be created
    """
    wav_dir = os.path.join(target_dir, 'wav/')
    txt_dir = os.path.join(target_dir, 'txt/')
    os.makedirs(wav_dir, exist_ok=True)
    os.makedirs(txt_dir, exist_ok=True)
    path_to_data = os.path.dirname(txt_file)

    def process(x):
        file_path = x["audio_file"]
        text = x["transcription"]
        start_time = x["start_time"]
        duration = x["end_time"] - start_time
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        file_name = str(start_time) + "_" + str(duration) + file_name
        text = text.strip().upper()
        with open(os.path.join(txt_dir, file_name + '.txt'), 'w') as f:
            f.write(text)
        cmd = "sox -v 0.6 -t wav {} -r {} -b 16 -c 1 -t wav {} trim {} {}".format(
                os.path.join(path_to_data, file_path),
                args.sample_rate,
                os.path.join(wav_dir, file_name + ".wav"),
                start_time,
                duration)
        subprocess.call([cmd], shell=True)
    print('Converting wav to wav for {}.'.format(txt_file))
    # generate processed data
    data = read_transcription_file(txt_file, sph_path)
    with ThreadPool(10) as pool:
        pool.map(process, data)


def main():
    target_dir = args.target_dir
    os.makedirs(target_dir, exist_ok=True)

    target_unpacked_dir = os.path.join(target_dir, "CV_unpacked")
    os.makedirs(target_unpacked_dir, exist_ok=True)

    if args.data_dir and os.path.exists(args.data_dir):
        print('Find existing file {}'.format(args.data_dir))
    else:
        raise RuntimeError("Could not find downloaded IARPA babel corpus, please download the relevant corpus from LDC")
        
    if os.path.isdir(args.data_dir):
        print("Identified unpacked IARPA dataset")
        unpacked_location = args.data_dir
    else:
        print("Unpacking corpus to {} ...".format(target_unpacked_dir))
        tar = tarfile.open(target_file)
        tar.extractall(target_unpacked_dir)
        tar.close()
        unpacked_location = target_unpacked_dir

    path_flattened = re.sub(r"[\/]", "_", os.path.splitext(args.data_dir)[0])
    os.makedirs(os.path.join(target_dir, path_flattened), exist_ok=True)
    roots = {}
    # collect all the filepaths
    for root, dirs, files in os.walk(unpacked_location):        
        roots[root] = files
    
    audio_trans_pairs = [] # this is a list of tuples
    for root in roots:
        # find all the audio directories
        if re.search(r"/audio", root):
            transcription_root = re.sub(r"/audio", "/transcription", root)
            print(transcription_root)
            for fp in roots[root]:
                txt_fp = re.sub(r"\.wav", ".txt", fp)
                if os.path.exists(os.path.join(transcription_root, txt_fp)):
                    pair_tuple = (os.path.join(transcription_root, txt_fp),
                                  os.path.join(root, fp))
                    audio_trans_pairs.append(pair_tuple)
                
    for txt_path, audio_path in audio_trans_pairs:
        convert_to_wav(txt_path,
                        audio_path, 
                        os.path.join(target_dir,path_flattened))
        
    # make a separate manifest for each 
    print('Creating manifests...')
    create_manifest(os.path.join(target_dir,path_flattened),
                    path_flattened + '_manifest.csv',
                    args.min_duration,
                    args.max_duration)


if __name__ == "__main__":
    main()
