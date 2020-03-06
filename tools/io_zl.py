# Import standard libraries.
import os
import sys
import datetime as dt
# Import third party libraries.
import numpy as np
import pandas as pd
# Import module libraries.
from . import transform, validation, variables
# Import custom libraries.
import pyhecdss

pyhecdss.set_message_level(0)
pyhecdss.set_program_name('PYTHON')

def parse_filepaths(fp, studies=None):
    # Check that inputs provided are compatible and zip data into list.
    if isinstance(fp, str) and (isinstance(studies, str) or not studies):
        study_fps = [(studies, fp)]
    elif isinstance(fp, list) and (isinstance(studies, list) or not studies):
        if studies and (len(fp) != len(studies)):
            msg = ('List length of file paths `fp` must equal list length of'
                   ' study names.')
            raise TypeError(msg)
        if not studies:
            studies = ['Alt{}'.format(i) for i in range(len(fp))]
        study_fps = list(zip(studies, fp))
    else:
        msg = 'Inputs provided are incompatible.'
        raise TypeError(msg)
    return study_fps

def read_dss_catalog(fp, a=None, b=None, c=None, e=None, f=None, studies=None,
                     match=True):
    # Iterate through file paths.
    study_fps = parse_filepaths(fp, studies=studies)
    list_DSS_Cat = list()
    for study, f_path in study_fps:
        # Check if DSS file exists.
        if not os.path.exists(f_path):
            msg = 'File {} does not exist.'
            raise RuntimeError(msg.format(f_path))
        # Get pathlist from DSS Catalog given absolute or relative file path.
        # Returns a pandas dataframe with the *.dss pathnames broken-up into
        # columns from /A/B/C/D/E/F/
        # This is filter-able by pandas column logic
        dss_file_obj = pyhecdss.DSSFile(f_path)
        catalog_df = dss_file_obj.read_catalog()
        print(catalog_df)
        del catalog_df['T']
        del catalog_df['D']
        catalog_df.columns = ['Part A', 'Part B', 'Part C', 'Part F', 'Part E']
        # Filter by input search criteria.
        filter = catalog_df.any(axis=1)
        filters = dict()
        if a: filters['Part A'] = a if isinstance(a, list) else [a]
        if b: filters['Part B'] = b if isinstance(b, list) else [b]
        if c: filters['Part C'] = c if isinstance(c, list) else [c]
        if e: filters['Part E'] = e if isinstance(e, list) else [e]
        if f: filters['Part F'] = f if isinstance(f, list) else [f]
        if filters:
            for k, v in filters.items():
                v_upper = [x.upper() for x in v]
                if match:
                    filter = filter & catalog_df[k].isin(v_upper)
                else:
                    v_search = '|'.join(v_upper)
                    filter = filter & catalog_df[k].str.contains(v_search,
                                                                regex=True)
        # Apply filter and ensure values are returned.
        DSS_Cat_f = catalog_df.loc[filter, :]
        if DSS_Cat_f.empty:
            msg = 'No pathnames returned from provided filter criteria.'
            raise TypeError(msg)

        DSS_Cat_f['File Path'] = f_path
        if study: DSS_Cat_f['Study'] = study
        # Add to list.
        list_DSS_Cat.append(DSS_Cat_f.copy())
        dss_file_obj.close()
    # Concatenate list of DataFrames into a single DataFrame.
    DSS_Cat = pd.concat(list_DSS_Cat).reset_index(drop=True)
    # Remove duplicate pathnames.
    duplicates = DSS_Cat.duplicated()
    DSS_Cat = DSS_Cat.loc[~duplicates, :]
    DSS_Cat.reset_index(drop=True, inplace=True)
    # Reconstruct pathnames.
    transform.join_pathname(DSS_Cat, inplace=True, e='skip')
    # Return filtered catalog.
    return DSS_Cat

def read_dss(fp, start_date='1921-10-31', end_date='2003-09-30',
             supp_info=False, **kwargs):
    # Acquire catalog of pathnames.
    dss_cat = read_dss_catalog(fp, **kwargs)
    # Get list of studies and file paths.
    study_fps = transform.cat_to_study_fps(dss_cat)
    list_df = list()
    # Read data into DataFrame.
    for study, f_path in study_fps:
        # Get pathnames for current file.
        fp_filt = (dss_cat['File Path'] == f_path)
        st_filt = (dss_cat['Study'] == study) if study else dss_cat.any(axis=1)
        cat_filter = (fp_filt & st_filt)
        s_pathnames = dss_cat.loc[cat_filter, 'Pathname']
        # Open Dss file
        dss_file_obj = pyhecdss.DSSFile(f_path)
        # Query data from DSS file.
        for row in s_pathnames.index:
            cpath = s_pathnames[row]
            temp_df, temp_unit, temp_type = dss_file_obj.read_rts(cpath, startDateStr=start_date, endDateStr=end_date)
            temp_df['Pathname'] = cpath
            temp_df['Units'] = temp_unit.upper()
            temp_df['Data Type'] = temp_type.upper()
            temp_df.reset_index(inplace=True)
            temp_df.rename(columns={cpath: 'Value', 'index': 'DateTime'}, inplace=True)
            list_df.append(temp_df.copy())
            print("Retrieved: {}".format(cpath))
        dss_file_obj.close()
    df = pd.concat(list_df, sort=True)
    df['Value'].replace([-901, -902], np.nan, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def write_dss(fp, df):
    # Make a copy of the input DataFrame.
    df_copy = df.copy()
    # Ensure DataFrame is tidy prior to writing.
    if not validation.is_tidy(df_copy):
        # TODO: In the future, write code to transform DataFrame.
        # <JAS 2019-09-13>
        msg = 'DataFrame is not in CalSim tidy format.'
        raise RuntimeError(msg)
    # Prepare for iteration over multiple DSS files.
    # TODO: Improve code below because, while stable, it is not end-user
    #       friendly or very readable code.
    # <JAS 2019-09-18>
    if 'Study' in df_copy.columns:
        if not isinstance(fp, dict):
            msg = ('Must provide dictionary of {study: file path} for DataFrame'
                   ' with "Study" column.')
            raise TypeError(msg)
        gv_st = list(fp.keys())
        ex_st = list(df_copy['Study'].unique())
        if (set(gv_st) != set(ex_st)) or (len(gv_st) != len(ex_st)):
            ms_st = set(ex_st) - set(gv_st)
            et_st = set(gv_st) - set(ex_st)
            msg = list()
            if ms_st:
                msg += [('The following studies are missing from `fp`'
                         ' dictionary: {}'.format(list(ms_st)))]
            if et_st:
                msg += [('The following studies do not exist in the'
                         ' DataFrame: {}'.format(list(et_st)))]
            raise RuntimeError('\n'.join(msg))
        study_fps = fp
    else:
        if not isinstance(fp, str):
            msg = ('Must provide file path as string (relative or absolute)'
                   ' for DataFrame.')
            raise TypeError(msg)
        study_fps = {None: fp}
    # Replace NaNs with -901.
    # ????: What is the difference between -901 and -902?
    # <JAS 2019-09-16>
    df_copy['Value'].fillna(-901, inplace=True)
    # Write to DSS file(s).
    for study, f_path in study_fps.items():
        # Open Dss file
        dss_file_obj = pyhecdss.DSSFile(f_path)
        st_filt = (df_copy['Study'] == study) if study else df_copy.any(axis=1)
        df_study = df_copy.loc[st_filt, :]
        list_pathname = df_study['Pathname'].unique()
        for pathname in list_pathname:
            pathname_filter = (df_study['Pathname'] == pathname)
            df_pathname = df_study.loc[pathname_filter, :].copy()
            df_pathname.sort_values('DateTime', inplace=True)
            df_pathname.reset_index(drop=True, inplace=True)
            cpath = pathname
            cdate = df_pathname['DateTime'].dt.strftime('%d%b%Y').values[0]
            ctime = df_pathname['DateTime'].dt.strftime('%H%M').values[0]
            vals = df_pathname['Value'].to_list()
            cunits = df_pathname['Units'].values[0]
            ctype = df_pathname['Data Type'].values[0]
            write_vals = np.array(vals).astype(np.float64)
            write_df = pd.DataFrame(write_vals, index=df_pathname['DateTime'], columns=[cpath])
            dss_file_obj.write_rts(cpath, write_df, cunits, ctype)
            print("Wrote {} to {}".format(cpath, f_path))
        dss_file_obj.close()
        if study:
            write_msg = 'Study {} successfully written to {} at {}.'
            print(write_msg.format(study, f_path, dt.datetime.now()))
        else:
            write_msg = 'DataFrame successfully written to {} at {}.'
            print(write_msg.format(f_path, dt.datetime.now()))
    # Return success indicator.
    return 0