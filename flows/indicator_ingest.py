import json
import os

tile_v0 = __import__('tile-v0')

root = os.getcwd()
jsons_dir = root + '/s3_jsons/'
indicator_metadata_files = os.listdir(jsons_dir)
indicator_metadata_files.sort()
failed_indicators = []
# indicator_metadata_files = ['UNICEF_Complementary_Feeding_Latest_Regional_Global.json']#, 'UNICEF_Continued_Breastfeeding_Latest_Regional_Global.json', 'UNICEF_Early_Initiation_Latest_Regional_Global.json', 'UNICEF_Exclusive_Breastfeeding_Latest_Regional_Global.json', 'UNICEF_ISSSF_Latest_Regional_Global.json']
for json_name in indicator_metadata_files:# [9891:]:
    if json_name[:3] != "UAZ":
        try:
            with open(jsons_dir + json_name) as json_file:
                indicator_json = json.loads(json_file.read())
                print("================================================================================")
                print(json_name)
                print("================================================================================")
                try:
                    tile_v0.run(indicator_json['id'], indicator_json['data_paths'], indicator_json['outputs'][0]['name'])
                except:
                    failed_indicators += [json_name]
                    print("================================================================================")
                    print(json_name + " failed")
                    print("================================================================================")
        except:
            pass

print("finished!")
