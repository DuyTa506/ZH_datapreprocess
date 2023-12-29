from pydub import AudioSegment
import os
import pandas as pd


def cut_and_save_audio(input_audio_path, input_data, output_folder):
    audio = AudioSegment.from_file(input_audio_path)
    os.makedirs(output_folder, exist_ok=True)

    data = pd.read_csv(input_data, sep='\t', header=None)
    filtered_data = data[~data[3].str.contains('\[\*\]|\[\+\]', regex=True, na=False)]
    
    for idx, ((start_time, end_time), transcript) in enumerate(zip(filtered_data[0].apply(eval), filtered_data[3])):
        start_time_ms = int(start_time * 1000)
        end_time_ms = int(end_time * 1000)

        cut_audio = audio[start_time_ms:end_time_ms]

        output_audio_path = os.path.join(output_folder, f'{start_time:.3f}_{end_time:.3f}.wav')
        cut_audio.export(output_audio_path, format="wav")

        output_transcripts_path = os.path.join(output_folder, f'{start_time:.3f}_{end_time:.3f}.txt')
        with open(output_transcripts_path, 'w', encoding='utf-8') as f:
            f.write(str(transcript))

        print(f'Đã cắt và lưu audio {idx} và transcripts vào: {output_audio_path}, {output_transcripts_path}')



def process_audio_text_pairs(audio_folder, text_folder, output_parent_folder):
    audio_files = [f for f in os.listdir(audio_folder) if f.endswith('.wav')]

    for audio_file in audio_files:
        audio_path = os.path.join(audio_folder, audio_file)

        text_file = os.path.splitext(audio_file)[0] + '.txt'
        text_path = os.path.join(text_folder, text_file)
        if os.path.exists(text_path):
            last_number = int(text_file.split('-')[-1].split('.')[0])
            output_folder = os.path.join(output_parent_folder, str(last_number))
            os.makedirs(output_folder, exist_ok=True)
            cut_and_save_audio(audio_path, text_path, output_folder)


if __name__ == "__main__":
    prefix = "./ASR-RAMC-BIGCCSC/MDT2021S003/"
    audio_folder = 'WAV'
    text_folder = 'TXT'
    output_folder = './cleaned_ASR-RAMC'
    process_audio_text_pairs(prefix+audio_folder, prefix+text_folder, output_folder)