import argparse
import os
import shutil
import threading
import ijson
from pydub import AudioSegment
from tqdm import tqdm

# Định nghĩa các tham số dòng lệnh
parser = argparse.ArgumentParser(description="Chuẩn bị dữ liệu từ tập dữ liệu WenetSpeech.")
parser.add_argument('--wenetspeech_json', type=str, default='/app/WenetSpeech/WenetSpeech/WenetSpeech.json',
                    help="Đường dẫn đến tệp JSON chứa thông tin của WenetSpeech.")
parser.add_argument('--annotation_dir', type=str, default='/app/ZH_datapreprocess/WenetSpeech/Cleaned_data',
                    help="Thư mục chứa dữ liệu đã được xử lý.")
parser.add_argument('--to_wav', type=bool, default=True,
                    help="Chuyển đổi định dạng âm thanh từ opus sang wav.")
parser.add_argument('--num_workers', type=int, default=24,
                    help="Số luồng xử lý đồng thời khi chuyển đổi âm thanh.")
args = parser.parse_args()

# Kiểm tra và tạo thư mục chứa dữ liệu đã xử lý
if not os.path.exists(args.annotation_dir):
    os.makedirs(args.annotation_dir)

# Đường dẫn đến tệp JSON chứa thông tin của WenetSpeech
wenetspeech_json = args.wenetspeech_json

# Tạo tên cho tệp chứa dữ liệu đã xử lý
train_list_path = os.path.join(args.annotation_dir, 'wenetspeech.json')
f_ann = open(train_list_path, 'a', encoding='utf-8')
test_list_path = os.path.join(args.annotation_dir, 'test.json')
f_ann_test = open(test_list_path, 'a', encoding='utf-8')

# Khóa tài nguyên cho việc ghi dữ liệu từ các luồng
threadLock = threading.Lock()
threads = []


class myThread(threading.Thread):
    def __init__(self, threadID, data):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.data = data

    def run(self):
        print(f"Khởi động luồng: {self.threadID}, số lượng dữ liệu: {len(self.data)}" )
        for i, data in enumerate(self.data):
            long_audio_path, segments_lists = data
            print(f'Luồng: {self.threadID} đang xử lý: [{i+1}/{len(self.data)}]')
            lines = process_wenetspeech(long_audio_path, segments_lists)
            # Khóa để tránh xung đột khi ghi dữ liệu
            threadLock.acquire()
            for line in lines:
                if long_audio_path.split('/')[-4] != 'train':
                    f_ann_test.write('{}\n'.format(str(line).replace("'", '"')))
                else:
                    f_ann.write('{}\n'.format(str(line).replace("'", '"')))
                f_ann.flush()
                f_ann_test.flush()
            # Mở khóa
            threadLock.release()
        print(f"Luồng: {self.threadID} đã hoàn thành")


# Xử lý dữ liệu từ WenetSpeech
def process_wenetspeech(long_audio_path, segments_lists):
    save_audio_path = long_audio_path.replace('.opus', '.wav')
    source_wav = AudioSegment.from_file(long_audio_path)
    target_audio = source_wav.set_frame_rate(16000)
    print(f"---------------{save_audio_path}------------------")
    target_audio.export(save_audio_path, format="wav")
    lines = []
    for segment_file in segments_lists:
        try:
            subsets = segment_file['subsets']
            confidence = segment_file['confidence']
            if 'M' in subsets and confidence == 1.0:
        
                start_time = float(segment_file['begin_time'])
                end_time = float(segment_file['end_time'])
                text = segment_file['text']
                
                # if confidence < 0.95:
                #     continue
        except Exception:
            print(f'''Cảnh báo: {segment_file} có lỗi, bỏ qua''')
            continue
        else:
            line = dict(audio_filepath=save_audio_path,
                        text=text,
                        duration=round(end_time - start_time, 3),
                        start_time=round(start_time, 3),
                        end_time=round(end_time, 3))
            lines.append(line)
    # Xóa file âm thanh đã xử lý
    os.remove(long_audio_path)
    return lines


# Lấy thông tin từ tệp JSON chứa dữ liệu WenetSpeech
def get_data(wenetspeech_json):
    data_list = []
    input_dir = os.path.dirname(wenetspeech_json)
    i = 0
    # Bắt đầu đọc dữ liệu
    with open(wenetspeech_json, 'r', encoding='utf-8') as f:
        objects = ijson.items(f, 'audios.item')
        print("Bắt đầu đọc dữ liệu")
        while True:
            try:
                long_audio = objects.__next__()
                i += 1
                try:
                    long_audio_path = os.path.realpath(os.path.join(input_dir, long_audio['path']))
                    aid = long_audio['aid']
                    segments_lists = long_audio['segments']
                    assert (os.path.exists(long_audio_path))
                except AssertionError:
                    print(f'''Cảnh báo: {long_audio_path} không tồn tại hoặc đã được xử lý và tự động xóa, bỏ qua''')
                    continue
                except Exception:
                    print(f'''Cảnh báo: Lỗi khi đọc dữ liệu của {aid}, bỏ qua''')
                    continue
                else:
                    data_list.append([long_audio_path.replace('\\', '/'), segments_lists])
            except StopIteration:
                print("Hoàn tất việc đọc dữ liệu")
                break
    return data_list


def main():
    all_data = get_data(args.wenetspeech_json)
    print(f'Tổng số dữ liệu: {len(all_data)}')
    if args.to_wav:
        text = input(f'File âm thanh sẽ được chuyển đổi sang định dạng wav. Quá trình này có thể mất thời gian và kích thước file cuối cùng gần 5.5T. Bạn có muốn tiếp tục không? (y/n)')
        if text is None or text != 'y':
            return
        chunk_len = len(all_data) // args.num_workers
        for i in range(args.num_workers):
            sub_data = all_data[i * chunk_len: (i + 1) * chunk_len]
            thread = myThread(i, sub_data)
            thread.start()
            threads.append(thread)

        # Đợi cho tất cả các luồng hoàn thành
        for t in threads:
            t.join()

        # Sao chép tệp chú thích, vì một số tệp chú thích đã được chuyển đổi sang định dạng wav
        input_dir = os.path.dirname(args.wenetspeech_json)
        shutil.copy(train_list_path, os.path.join(input_dir, 'wenetspeech_train.json'))
        shutil.copy(test_list_path, os.path.join(input_dir, 'wenetspeech_test.json'))
    else:
        text = input(f'Sử dụng trực tiếp định dạng opus. Lưu ý rằng việc đọc opus sẽ chậm hơn nhiều so với định dạng wav. Bạn có muốn tiếp tục không? (y/n)')
        if text is None or text != 'y':
            return
        for data in tqdm(all_data):
            long_audio_path, segments_lists = data
            for segment_file in segments_lists:
                start_time = float(segment_file['begin_time'])
                end_time = float(segment_file['end_time'])
                text = segment_file['text']
                confidence = segment_file['confidence']
                if confidence < 0.95:
                    continue
                line = dict(audio_filepath=long_audio_path,
                            text=text,
                            duration=round(end_time - start_time, 3),
                            start_time=round(start_time, 3),
                            end_time=round(end_time, 3))
                if long_audio_path.split('/')[-4] != 'train':
                    f_ann_test.write('{}\n'.format(str(line).replace("'", '"')))
                else:
                    f_ann.write('{}\n'.format(str(line).replace("'", '"')))
    f_ann.close()
    f_ann_test.close()


if __name__ == '__main__':
    main()
