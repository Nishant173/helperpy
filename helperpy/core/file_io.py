from typing import Any, List, Optional
import json
import ntpath
import os
import shutil

import joblib
import numpy as np
import pandas as pd
from tinytag import TinyTag
from tqdm import tqdm

from helperpy.core.utils import get_timetaken_fstring
from helperpy.data_wrangler.transform import rank_and_sort


def get_extension(filepath: str) -> str:
    return os.path.splitext(filepath)[-1][1:]


def get_basename_from_filepath(filepath: str) -> str:
    """Returns base-name of the file/folder from the given `filepath` (along with the extension, if any)"""
    head, tail = ntpath.split(p=filepath)
    return tail or ntpath.basename(head)


def get_absolute_filepath(filepath: str) -> str:
    """Returns absolute filepath of the file/folder from the given `filepath` (along with the extension, if any)"""
    absolute_filepath = os.path.realpath(path=filepath)
    return absolute_filepath


def get_line_count(filepath: str) -> int:
    """Returns count of number of lines in the given file"""
    num_lines = sum(1 for _ in open(file=filepath, encoding="utf8"))
    return num_lines


def filter_filepaths_by_extensions(
        filepaths: List[str],
        extensions: List[str],
    ) -> List[str]:
    """
    Filters given filepaths by the desired extensions.

    >>> filter_filepaths_by_extensions(
        filepaths=['one.js', 'two.py', 'three.css', 'four.go', 'five.html', 'six.py', 'seven.js'],
        extensions=['css', 'js'],
    )
    """
    extensions = list(
        map(lambda extension: extension.strip().lower(), extensions)
    )
    filepaths_needed = list(
        filter(lambda filepath: get_extension(filepath=filepath).strip().lower() in extensions, filepaths)
    )
    return filepaths_needed


def get_unique_extensions(filepaths: List[str]) -> List[str]:
    """Returns all unique extensions available in the list of filepaths given"""
    all_extensions = list(map(get_extension, filepaths))
    unique_extensions = sorted(list(set(all_extensions)))
    return unique_extensions


def __get_filepaths_at_first_level(src_dir: str) -> List[str]:
    folders_and_files_in_directory = os.listdir(src_dir)
    folders_and_files_in_directory = list(
        map(lambda folder_or_file: os.path.join(src_dir, folder_or_file), folders_and_files_in_directory)
    )
    files_in_directory = list(
        filter(lambda folder_or_file: os.path.isfile(folder_or_file), folders_and_files_in_directory)
    )
    return files_in_directory


def __get_filepaths_at_all_levels(src_dir: str) -> List[str]:
    filepaths = []
    for path, _, filenames in os.walk(src_dir):
        for filename in filenames:
            filepath = os.path.join(path, filename)
            filepaths.append(filepath)
    return filepaths


def get_filepaths(
        src_dir: str,
        depth: str,
        extensions: Optional[List[str]] = None,
    ) -> List[str]:
    """
    Gets list of all filepaths (of files) from source directory.
    
    Parameters:
        - src_dir (str): Filepath to the source directory. Can be absolute or relative.
        - depth (str): Options: ['all_levels', 'first_level'].
        Set to 'all_levels' if you want to get filepaths from all sub-directories (if any) in the given source directory.
        Set to 'first_level' if you want to get filepaths only from the first directory given.
        - extensions (list): List of extensions to filter the filepaths by (optional).
    
    >>> get_filepaths(
            src_dir="SOME_SOURCE_DIR",
            depth='all_levels',
            extensions=['csv', 'xlsx'],
        )
    """
    depth_options = ['all_levels', 'first_level']
    if depth not in depth_options:
        raise ValueError(f"Expected `depth` to be in {depth_options}, but got '{depth}'")
    
    if depth == 'all_levels':
        filepaths = __get_filepaths_at_all_levels(src_dir=src_dir)
    elif depth == 'first_level':
        filepaths = __get_filepaths_at_first_level(src_dir=src_dir)
    if extensions is not None:
        filepaths = filter_filepaths_by_extensions(filepaths=filepaths, extensions=extensions)
    return filepaths


def create_archive_file(
        src_dir: str,
        archive_format: str,
    ) -> None:
    """
    Creates archive file of the given source directory.
    Options for `archive_format` are: ['zip', 'tar', 'gztar', 'bztar', 'xztar'].
    """
    absolute_path_to_src_dir = get_absolute_filepath(filepath=src_dir)
    basename_of_src_dir = get_basename_from_filepath(filepath=src_dir)
    shutil.make_archive(
        base_name=basename_of_src_dir,
        format=archive_format,
        root_dir=absolute_path_to_src_dir,
    )
    return None


def get_line_count_info(
        filepaths: List[str],
        rank_by_line_count: Optional[bool] = False,
    ) -> pd.DataFrame:
    """
    Gets line-count of all filepaths given.
    Returns DataFrame having columns: ['Filepath', 'LineCount', 'Basename', 'Extension'].
    If `rank_by_line_count` is set to True, then there will be an additional column called: 'RankByLineCount'.

    >>> get_line_count_info(
            filepaths=['file1.txt', 'file2.txt', 'file3.txt'],
            rank_by_line_count=True,
        )
    """
    df = pd.DataFrame()
    df['Filepath'] = filepaths
    df['LineCount'] = df['Filepath'].apply(get_line_count)
    df['Basename'] = df['Filepath'].apply(get_basename_from_filepath)
    df['Extension'] = df['Filepath'].apply(get_extension)
    if rank_by_line_count:
        df = rank_and_sort(
            data=df,
            rank_column_name='RankByLineCount',
            rank_by=['LineCount'],
            ascending=[False],
            how='dense_rank',
        )
    return df


def get_media_metadata(filepaths: List[str]) -> pd.DataFrame:
    """
    Gets metadata of all media-file filepaths given i.e; files with extensions mp3, mp4, etc.
    Returns DataFrame having columns: ['Filepath', 'Basename', 'Extension', 'BitRate',
    'DurationInSeconds', 'Duration', 'SizeInMb', 'SampleRate'].
    
    >>> get_media_metadata(filepaths=['audio.mp3', 'audio.m4a', 'video.mp4'])
    """
    bytes_per_mb = 1048576
    df = pd.DataFrame()
    df['TinyTagObject'] = [TinyTag.get(filepath) for filepath in tqdm(filepaths)]
    df['Filepath'] = filepaths
    df['Basename'] = df['Filepath'].apply(get_basename_from_filepath)
    df['Extension'] = df['Filepath'].apply(get_extension)
    df['BitRate'] = df['TinyTagObject'].apply(lambda obj: obj.bitrate)
    df['DurationInSeconds'] = df['TinyTagObject'].apply(
        lambda obj: None if obj.duration is None else int(np.ceil(obj.duration))
    )
    df['Duration'] = df['TinyTagObject'].apply(
        lambda obj: None if obj.duration is None else get_timetaken_fstring(
            num_seconds=int(np.ceil(obj.duration))
        )
    )
    df['SizeInMb'] = df['TinyTagObject'].apply(
        lambda obj: None if obj.filesize is None else round(obj.filesize / bytes_per_mb, 2)
    )
    df['SampleRate'] = df['TinyTagObject'].apply(lambda obj: obj.samplerate)
    df.drop(labels=['TinyTagObject'], axis=1, inplace=True)
    return df


def pickle_load(filepath: str) -> Any:
    """Loads data from pickle file, via joblib module"""
    python_obj = joblib.load(filename=filepath)
    return python_obj


def pickle_save(data_obj: Any, filepath: str) -> None:
    """Saves data as pickle file, via joblib module"""
    joblib.dump(value=data_obj, filename=filepath)
    return None


def save_object_as_json(obj: Any, filepath: str) -> None:
    """Saves Python object as JSON file"""
    with open(file=filepath, mode='w') as fp:
        json.dump(obj=obj, fp=fp, indent=4)
    return None


def read_to_dataframe(filepath: str) -> pd.DataFrame:
    """
    Reads DataFrame from filepaths having any of the following extensions: ['csv', 'xlsx'].
    >>> df = read_to_dataframe(filepath="some_data.csv")
    """
    extension = get_extension(filepath=filepath).lower()
    if extension == 'csv':
        data = pd.read_csv(filepath)
    elif extension == 'xlsx':
        data = pd.read_excel(filepath)
    else:
        raise ValueError(f"Expected filepath's extension to be in ['csv', 'xlsx'], but got '{extension}'")
    return data


def save_dataframe(
        data: pd.DataFrame,
        filepath: str,
    ) -> None:
    """
    Saves DataFrame to one of the following file types: ['csv', 'xlsx', 'json'].
    >>> save_dataframe(
            data=pd.DataFrame(data={'Column1': [1, 2, 3, 4]}), # Some DataFrame
            filepath="some_data.csv",
        )
    """
    extension = get_extension(filepath=filepath).lower()
    if extension == 'csv':
        data.to_csv(filepath, index=False)
    elif extension == 'xlsx':
        data.to_excel(filepath, index=False)
    elif extension == 'json':
        data.to_json(filepath, orient='records', indent=4)
    else:
        raise ValueError(f"Expected filepath's extension to be in ['csv', 'xlsx', 'json'], but got '{extension}'")
    return None