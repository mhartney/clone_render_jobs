import csv

from pathlib import Path


def get_filepath(parent_folder: str, filename:str) -> str:
    """Return path based on location of main file, user
    provides parent folder / filename."""
    parent_dir = Path(__file__).resolve().parent.parent
    filepath = parent_dir / parent_folder / filename
    return filepath


def read_file(filepath:str) -> list:
    """Read input file and return list
    of lines from file."""
    with open(filepath, "r") as f:
        list = [line.strip() for line in f]
    return list


def write_to_csv(dict_row:dict, csv_path):
    """Writes a dictionary row to csv."""
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=dict_row.keys())
        writer.writerow(dict_row)


def create_csv(dict_row:dict, csv_path:str):
    """Create csv and only write header."""
    header = list(dict_row.keys())
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)


def write_id_to_file(filepath:str, id, new_id=None):
    """Write a ID and append to a text file. Use for
    writing IDs for new and old dgraphs."""
    with open(filepath, "a") as f:
        if new_id:
            f.write(f"{id},{new_id}\n")
        else:
            f.write(f"{id}\n")


def get_dgraph_list(input_id_path: str, output_id_path: str) -> list:
    """Compare two lists of IDs and remove entries 
    to a new list that have not already been cloned."""
    dgraph_ids = read_file(input_id_path)
    cloned_ids = read_file(output_id_path)
    
    cleaned_lines = [i for i in dgraph_ids
                     if i not in cloned_ids]
    
    return cleaned_lines


def write_new_file(filepath: str, edited_list:list):
    with open(filepath, "w") as f:
        for i in edited_list:
            f.write(f"{i}\n")
