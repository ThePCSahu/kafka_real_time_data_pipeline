from datetime import datetime
import json

# Custom encoder for serialization of datetime objects
def datetime_encoder(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    
# Writer class for writing to json file
class CsvFileWriter:
    def __init__(self, file_name):
        self.file_name = file_name
        

    def write(self, new_data):
        try:
            with open(self.file_name, 'a') as json_file:
                json.dump(new_data, json_file, default=datetime_encoder)
                json_file.write('\n')
                json_file.flush()
        except Exception as e:
            print(f"Error occured while appending to file {self.file_name}")
        print(f"New data has been appended to {self.file_name}")


