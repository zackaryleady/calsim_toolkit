LIB_DSS = None
import pyhecdss
LIB_DSS = "pyhecdss"
from . import io_zl
# try:
#     import pyhecdss
#     LIB_DSS = "pyhecdss"
#     #pyhecdss.set_message_level(10)
#     #pyhecdss.set_program_name('PYTHON')
#     from . import io_zl
# except:
#     try:
#         import dss3_functions_reference as dss
#         LIB_DSS = "dss3_jg"
#         from . import io_js
#     except:
#         msg = ("An external library for readin *.dss files such as pyhecdss or dss3_jg needs to be available. Neither detected.")
#         raise ImportError(msg)


def parse_filepaths(fp, studies=None):
    if LIB_DSS == "pyhecdss":
        study_fps = io_zl.parse_filepaths(fp, studies=None)
    else:
        study_fps = io_js.parse_filepaths(fp, studies=None)
    return study_fps

def read_dss_catalog(fp, a=None, b=None, c=None, e=None, f=None, studies=None,
                     match=True):
    if LIB_DSS == "pyhecdss":
        DSS_Cat = io_zl.read_dss_catalog(fp, a=None, b=None, c=None, e=None, f=None, studies=None,
                                         match=True)
    else:
        DSS_Cat = io_js.read_dss_catalog(fp, a=None, b=None, c=None, e=None, f=None, studies=None,
                                         match=True)
    
    # Return filtered catalog.
    return DSS_Cat

def read_dss(fp, start_date='1921-10-31', end_date='2003-09-30',
             supp_info=False, **kwargs):
    if LIB_DSS == "pyhecdss":
        df = io_zl.read_dss(fp, start_date='1921-10-31', end_date='2003-09-30',
                            supp_info=False, **kwargs)
    else:
        df = io_js.read_dss(fp, start_date='1921-10-31', end_date='2003-09-30',
                            supp_info=False, **kwargs)
    return df

def write_dss(fp, df):
    if LIB_DSS == "pyhecdss":
        io_zl.write_dss(fp, df)
    else:
        io_js.write_dss(fp_df)
    return 0