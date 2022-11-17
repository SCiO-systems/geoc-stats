import numpy as np
import numpy.ma as ma
import gdal
import os
import sys
import pathlib
import boto3
import json
import copy
import time

# import requests
s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    
    body = json.loads(event['body'])
    json_file = body

    path_to_save_temp_files = "/tmp/"

    # get  input json and extract geojson
    try:
        geojson_dict = json_file["target"]
        datasets = json_file["datasets"]
    except Exception as e:
        print(e)

    if bool(geojson_dict) == False:
        raise Exception("Empty GeoJSON")

    all_datasets_dict = {}
    datasets_used = []
    for dataset in datasets:

        all_datasets_dict[dataset["id"]] = dataset
        all_datasets_dict[dataset["id"]]["statistics"] = {}

        if dataset["chosen"] == True:
            datasets_used.append(dataset)

    gdal_warp_kwargs = {
        'format': 'GTiff',
        'cutlineDSName': json.dumps(geojson_dict),
        'cropToCutline': True,
        'dstNodata': -32768,
        # 'creationOptions': ['COMPRESS=DEFLATE']

    }
    start_time = time.time()
    # get area data from s3 based on geojson and files that can be processed with mean function
    for dataset in datasets_used:
        # print(dataset)
        # create the paths used
        s3_file_path = '/vsis3/geoc-slm-function-data/' + dataset["filename"]
        # discard cog_ at the bigging of the file name
        local_save_file_name = dataset["filename"].split("_")[1]
        save_temp_file_path = path_to_save_temp_files + local_save_file_name
        # execute the data transfer and save result
        try:
            gdal.Warp(save_temp_file_path, s3_file_path, **gdal_warp_kwargs)
        except:
            print("Error in gdalWarp, probably not finding the files to read")
    end_time = time.time()
    
    # return {
    #     "statusCode": 200,
    #     "body": str(end_time-start_time)
    # }
    
    # output json bulding blocks
    numeric_output_block = {
        "dataset": None,
        "id": None,
        "avg": None,
        "max": None,
        "min": None,
        "std": None,
        "unit": None
    }

    categorical_output_block = {
        "dataset": None,
        "id": None,
        "percentages": {

        },
        "unit": None
    }

    # process all files
    for dataset in datasets_used:
        # open files
        file_array = gdal.Open(path_to_save_temp_files + dataset["filename"].split("_")[1]).ReadAsArray()
        # mask no data values
        file_array = ma.array(file_array, mask=np.logical_or(file_array <= -9999, file_array == 255), fill_value=-9999)
        if dataset["type"] == "numerical":

            # compute mean
            mean_value = float(np.mean(file_array))
            max_value = float(np.amax(file_array))
            min_value = float(np.amin(file_array))
            std_value = float(np.std(file_array))

            # fill the building block
            numeric_output_block["dataset"] = dataset["filename"]
            numeric_output_block["id"] = dataset["id"]
            numeric_output_block["avg"] = mean_value
            numeric_output_block["max"] = max_value
            numeric_output_block["min"] = min_value
            numeric_output_block["std"] = std_value
            numeric_output_block["unit"] = dataset["unit"]

            output_block = numeric_output_block

        else:

            unique, counts = np.unique(file_array, return_counts=True)
            if type(unique[-1]) == np.ma.core.MaskedConstant:
                unique = unique[:-1]
                counts = counts[:-1]

            if dataset["unit"] == "7_classes":
                if 0 in unique:
                    unique = unique[1:]
                    counts = counts[1:]
                all_classes = ["1", "2", "3", "4", "5", "6", "7"]

            elif dataset["unit"] == "12_classes":
                all_classes = ["100", "200", "300", "400", "500", "600", "700", "800", "900", "1000", "1100", "1200"]

            else:
                all_classes = ["0", "1"]

            unique = [str(x) for x in unique]
            categorical_output_block["percentages"] = dict(zip(all_classes, [None] * len(all_classes)))

            total_sum = sum(counts)
            percentages = [round(100 * x / total_sum, 2) for x in counts]
            for i in range(len(unique)):
                categorical_output_block["percentages"][str(unique[i])] = percentages[i]

            categorical_output_block["dataset"] = dataset["filename"]
            categorical_output_block["id"] = dataset["id"]
            categorical_output_block["unit"] = dataset["unit"]

            output_block = categorical_output_block

        # append block to the final list for the json output
        all_datasets_dict[dataset["id"]]["statistics"] = copy.copy(output_block)

    my_output = {
        "results": []
    }

    for key, value in all_datasets_dict.items():
        my_output["results"].append(value)


    return {
        "statusCode": 200,
        "body": json.dumps(my_output)
    }


