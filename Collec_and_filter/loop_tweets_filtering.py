import subprocess

def call_filtering_script(start_number, end_number):
    for num in range(start_number, end_number + 1):
        print(f"Processing directory number: {num}")
        subprocess.run(['python', 'tweets_filtering.py', str(num)], check=True)

if __name__ == '__main__':
    start_number = 7  # Set the starting number
    end_number = 7   # Set the ending number
    call_filtering_script(start_number, end_number)
