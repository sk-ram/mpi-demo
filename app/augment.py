from logging import Logger
import logging
from typing import List
from app.sdk.models import KernelPlancksterSourceData, BaseJobState, JobOutput, ProtocolEnum
from app.sdk.scraped_data_repository import ScrapedDataRepository,  KernelPlancksterSourceData
import time
import os
import json
import pandas as pd
from dotenv import load_dotenv
from models import PipelineRequestModel
from numpy import ndarray
import numpy as np
import cv2
import tempfile

# Load environment variables
load_dotenv()


def augment(
    job_id: int,
    tracer_id: str,
    scraped_data_repository: ScrapedDataRepository,
    log_level: Logger,
    work_dir: str


) -> JobOutput:

    
    try:
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=log_level)

        job_state = BaseJobState.CREATED
        protocol = scraped_data_repository.protocol
    
        # Set the job state to running
        logger.info(f"{job_id}: Starting Job")
        job_state = BaseJobState.RUNNING
        #job.touch()

     
        #Download all relevant files from minio
        kernel_planckster = scraped_data_repository.kernel_planckster
        source_list = kernel_planckster.list_all_source_data()

        for source in source_list:
            download_source_if_relevant(source, job_id, tracer_id, scraped_data_repository, work_dir)


                
        
    except Exception as error:
        logger.error(f"{job_id}: Unable to scrape data. Job with tracer_id {tracer_id} failed. Error:\n{error}")
        job_state = BaseJobState.FAILED
        #job.messages.append(f"Status: FAILED. Unable to scrape data. {e}")




def download_source_if_relevant(source: KernelPlancksterSourceData, job_id:int, tracer_id: str, scraped_data_repository: ScrapedDataRepository, work_dir: str):
    
    name = source["name"]
    protocol = source["protocol"]
    relative_path = source["relative_path"]

    #reconstruct the source_data object
    source_data = KernelPlancksterSourceData(
        name= name,
        protocol=protocol,
        relative_path=relative_path,
    )

    key = {
    "01": "January",
    "02": "February",
    "03": "March",
    "04": "April",
    "05": "May",
    "06": "June",
    "07": "July",
    "08": "August",
    "09": "September",
    "10": "October",
    "11": "November",
    "12": "December"
    }

    file_name = os.path.basename(relative_path)         
    if "png" in file_name:
        underscore_date=file_name[:file_name.index("__")]
        split_date = underscore_date.split("_")
        sat_image_year = split_date[0]; sat_image_month = key[split_date[1]]; sat_image_day = split_date[2]
        file_name = f"{sat_image_year}_{sat_image_month}_{sat_image_day}.png"

    if "by_date" in source_data.relative_path and os.path.splitext(source_data.relative_path)[1] ==".json":
        path = os.path.join(work_dir, "by_date", file_name)
        scraped_data_repository.download_json(source_data, job_id, path)
    elif "true_color" in source_data.relative_path and os.path.splitext(source_data.relative_path)[1] ==".png":
        path = os.path.join(work_dir, "images", file_name)
        scraped_data_repository.download_img(source_data, job_id, path)
