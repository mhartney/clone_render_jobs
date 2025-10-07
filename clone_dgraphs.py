#!/usr/bin/env python

"""
Created by: Max Hartney
Date: 10/03/2025

This tool runs a cloning command on an input dgraph, and then
writes the new IDs information to a csv report. 
It reads the input IDs from a large list of jobs save on file, 
and updates as it runs so the user only needs to specify the 
number of IDs to clone when running. Previously cloned input
dgraphs are saved and skipped if input again. Metadata on new jobs
are configured for AWS Amazon testing.
"""

import time
import argparse

from datetime import datetime

from pwd import getpwuid
from codac.queue.edit import Edit
from codac.queue.query import Query
from codac.session import Session

from cloning_utils import file_management, helpers


def get_arg() -> int:
    parser = argparse.ArgumentParser(description="Clone IDs program")
    parser.add_argument("--clone-job-num", "-cj",
                        type=int, 
                        help="Enter number of ids you want to clone.", 
                        required=True)
    
    arg = parser.parse_args()
    print(f"\nCloning Job IDs: {arg.clone_job_num}".upper())
    return arg.clone_job_num


def get_ids_to_clone() -> list:
    """Compare lists of IDs used previously, and input IDs, 
    return list with no IDs used before."""
    print(f"INFO: Compiling ID list ...")
    file_dir = "id_text_files"
    input_id_path = file_management.get_filepath(file_dir, "input_ids_to_clone.txt")
    used_id_path = file_management.get_filepath(file_dir, "output_ids_cloned.txt")

    clean_ids = file_management.get_dgraph_list(input_id_path, used_id_path)
    input_ids = file_management.read_file(input_id_path)
    if clean_ids != input_ids:
        file_management.write_new_file(input_id_path, clean_ids)
    
    return clean_ids


def generate_cloned_jobs(input_ids: list, clone_num) -> list:
    """Generates cloned jobs and returns list of new IDs."""
    new_ids = []
    for num, input_id in enumerate(input_ids):
        if num >= clone_num:
            print("\nINFO: Cloning finished.")
            break
        
        # Note used ID.
        file_dir = "id_text_files"
        out_path = file_management.get_filepath(file_dir, "output_ids_cloned.txt")
        file_management.write_id_to_file(out_path, input_id)

        # Run clone command.
        cmd = ["coda-jobs", str(input_id), "clone", "--queued"]
        process = helpers.run_command(cmd)

        if "not found" in process:
            clone_num += 1
            print(f"\n--- New Job {num + 1} ---")
            print(f"INFO: skipping ID '{input_id}' as it is invalid.")
            continue
        else:
            new_id = process.split()[-1].strip()
            print(f"\n--- New Job {num + 1}: {new_id} ---")
            print(f"Original ID: {input_id}")
            new_path = file_management.get_filepath(file_dir, "new_ids.txt")
            file_management.write_id_to_file(new_path, input_id, new_id)

        if new_id:
            configure_metadata(new_id)
            new_ids.append((input_id, new_id)) 
        
        print(f"Finishing running at job number: {clone_num}")
        print("This number increases when we skip an invalid ID.")
        time.sleep(10)
    
    return new_ids


def configure_metadata(new_id):
    """Update metadata of new cloned jobs."""
    print("\nINFO: Configuring metadata for new ID.")
    codac_edit = Edit(Session())
    codac_query = Query(Session())
    
    dgraph = codac_query.get_dgraph_for_jobid(str(new_id))
    arrays = dgraph.arrays

    print("INFO: katana_render arrays:")
    for k, v in arrays.items():
        if v.phase == "katana_render":
            print(f"\nArray: {v.jobid}")
            client_prio = ["4"]
            codac_edit.set_clientpriolist_array(new_id, v._id, client_prio) 
            print(f"Setting 'clientpriolist' to {client_prio}")
            
            codac_edit.set_resource_object(v, "onpremonly", False)
            codac_edit.remove_resource_object(v, "onpremonly")
            print("Removing array resource 'onpremonly: true'")

            cpu_pool = f"{v.show}.cloud"
            codac_edit.set_cpu_pool_array(new_id, v._id, cpu_pool)
            print(f"Array added to cpupool '{cpu_pool}'")

            new_key = codac_edit._Edit__build_identifier("cloud_provider", new_id, v._id)
            codac_edit._Edit__api.set_meta(new_key, "Amazon AWS")
            print("Added metadata 'cloud_provider: Amazon AWS'")

    print("\nINFO: Resources updated.\n---")


def generate_cloning_report(input_id_list: list):
    """Generate report on new IDs."""
    if not input_id_list:
        print("INFO: Skipping csv generation ...\n")
        return
    csv_generated = False
    parent_dir = "cloning_reports"
    csv_filename =  f"cloning_report_{helpers.time_stamp()}.csv"
    csv_path = file_management.get_filepath(parent_dir, csv_filename)

    for id, new_id in input_id_list:
        dgraph_info = collect_dgraph_info(id, new_id)
        
        if not csv_generated:
            file_management.create_csv(dgraph_info, csv_path)
            csv_generated = True
        
        file_management.write_to_csv(dgraph_info, csv_path)
    print(f"REPORT: {csv_path}\n")


def collect_dgraph_info(input_id, new_id) -> dict:
    """Check codamon for details on new cloned jobs
    and save on file for record."""
    codac_query = Query(Session())
    dgraph = codac_query.get_dgraph_for_jobid(str(new_id))
    
    dt = datetime.fromtimestamp(dgraph.submit_time)
    user = getpwuid(dgraph.user).pw_name
   
    dgraph_info_for_report = {
        "user": user,
        "Original Job ID": input_id,
        "New Job ID": dgraph.id,
        "Show": dgraph.show,
        "Shot": dgraph.shot,
        "Title": dgraph.title,
        "Submit Time": dt
                        }
    
    return dgraph_info_for_report


if __name__ == "__main__":
    clone_job_num = get_arg()

    input_ids = get_ids_to_clone()

    new_ids = generate_cloned_jobs(input_ids, clone_job_num)

    generate_cloning_report(new_ids)
        
