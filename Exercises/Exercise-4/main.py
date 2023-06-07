import boto3


def main():
    import os
    import json
    import csv

    def normalize_json(data: dict) -> dict:
        # Your implementation of the `normalize_json` function here
        new_data = dict()
        for key, value in data.items():
            if not isinstance(value, dict):
                new_data[key] = value
            else:
                for k, v in value.items():
                    new_data[key + "_" + k] = v
        return new_data
    def convert_json_to_csv(json_file_path: str, csv_file_path: str):
        with open(json_file_path, 'r') as json_file:
            json_data = json.load(json_file)

        flattened_data = normalize_json(json_data)

        with open(csv_file_path, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=flattened_data.keys())
            writer.writeheader()
            writer.writerow(flattened_data)

    def process_folder(folder_path: str):
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.json'):
                    json_file_path = os.path.join(root, file)
                    csv_file_path = os.path.splitext(json_file_path)[0] + '.csv'
                    convert_json_to_csv(json_file_path, csv_file_path)

    # Example usage:
    folder_path = 'B:\practice\daily_updates\Exercises\Exercise-4\data'  # Replace with the actual folder path
    process_folder(folder_path)

    pass


if __name__ == "__main__":
    main()