import json
import os
import subprocess
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import random
import string

output_path = "/app/WenetSpeech/Cleaned_data"
os.makedirs(output_path, exist_ok=True)

def find_opus_files(folder_path):
    opus_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".opus"):
                file_path = os.path.join(root, file)
                opus_files.append(file_path)
    return opus_files

def cut_opus_file_ffmpeg(input_path, output_path, start_time, end_time):
    try:
        subprocess.run([
            'ffmpeg',
            '-i', input_path,
            '-ss', str(start_time),
            '-to', str(end_time),
            '-c', 'copy',
            output_path
        ], check=True)

        print(f"Đã cắt file từ {start_time} giây đến {end_time} giây và lưu thành {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Có lỗi xảy ra: {e}")

def convert_opus_to_wav_ffmpeg(input_opus_path, output_wav_path, channels=1, sample_rate=16000):
    try:
        subprocess.run([
            'ffmpeg',
            '-i', input_opus_path,
            '-ac', str(channels),
            '-ar', str(sample_rate),
            output_wav_path
        ], check=True)

        print(f"Đã chuyển đổi file từ {input_opus_path} sang {output_wav_path} với kênh = {channels} và tần số lấy mẫu = {sample_rate} Hz")

        # Xóa file .opus cũ đi sau khi chuyển đổi
        os.remove(input_opus_path)
        print(f"Đã xóa file cũ {input_opus_path}")
    except subprocess.CalledProcessError as e:
        print(f"Có lỗi xảy ra: {e}")
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")

def find_element_containing_a(a, str_list):
    for element in str_list:
        if a in element:
            return element
    return None

def generate_random_string(length=7):
    letters_and_digits = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(letters_and_digits) for i in range(length))
    return random_string

def process_record(record, opus_file_paths):
        path = record.get("path")
        segments = record.get("segments")
        full_path = find_element_containing_a(path, opus_file_paths)
        for x in segments:
            if x.get('confidence')==1.0:
                begin_time = x.get('begin_time')
                end_time = x.get('end_time')
                random_string = generate_random_string()
                output_opus_path = f'{output_path}/{begin_time}_{end_time}_{random_string}.opus'
                output_wav_path = f'{output_path}/{begin_time}_{end_time}_{random_string}.wav'
                text = x.get('text')
                cut_opus_file_ffmpeg(full_path, output_opus_path, begin_time, end_time)
                convert_opus_to_wav_ffmpeg(output_opus_path, output_wav_path)
                with open(f'{output_path}/{begin_time}_{end_time}_{random_string}.txt', 'w', encoding='utf-8') as f:
                    f.write(text)
            else:
                continue

def get_duration_from_json(json_path):
    try:
        main_folder = '/app/WenetSpeech/WenetSpeech/audio'
        opus_file_paths = find_opus_files(main_folder)

        with open(json_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

            with ThreadPoolExecutor() as executor:
                future_to_record = {executor.submit(process_record, record, opus_file_paths): record for record in data}
                
                for future in concurrent.futures.as_completed(future_to_record):
                    record = future_to_record[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Xảy ra lỗi khi xử lý bản ghi: {e}")

    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")

def main():
    example_json_path = '/app/ZH_datapreprocess/WenetSpeech/2000_record.json'
    get_duration_from_json(example_json_path)

if __name__ == "__main__":
    main()
