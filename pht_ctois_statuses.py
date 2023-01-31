# Aggregate various sources of information for PHT CTOI

import pandas as pd

import download_utils

BTJD_REF = 2457000

TOI_CSV_URL = (
    "https://exofop.ipac.caltech.edu/tess/download_toi.php?sort=toi&output=csv"
)
CTOI_CSV_URL = (
    "https://exofop.ipac.caltech.edu/tess/download_ctoi.php?sort=ctoi&output=csv"
)


# DEFAULT_CACHE_POLICY = download_utils.CachePolicy.ALWAYS_USE
DEFAULT_CACHE_POLICY = download_utils.CachePolicy.TTL_IN_DAYS(7)

DATA_DIR = "data"
DOWNLOAD_DIR = "data/download"

TOI_CSV_LOCAL_FILENAME = "tess_tois.csv"
CTOI_CSV_LOCAL_FILENAME = "tess_ctois.csv"
PHT_CTOI_SECTORS_CSV_LOCAL_FILENAME = "pht_ctoi_sectors.csv"
PHT_CTOI_STATUSES_CSV_LOCAL_FILENAME = "pht_ctoi_statuses.csv"

def get_tess_tois(cache_policy_func=DEFAULT_CACHE_POLICY):
    csv_path = download_utils.download_file(
        TOI_CSV_URL,
        TOI_CSV_LOCAL_FILENAME,
        download_dir=DOWNLOAD_DIR,
        cache_policy_func=cache_policy_func,
    )

    dtypes_map = { "TOI": str}

    # force nullable integer columns
    for col in [
        "Master", "SG1A", "SG1B", "SG2", "SG3", "SG4", "SG5",  # priorities
        "Time Series Observations", "Spectroscopy Observations", "Imaging Observations",   # num. of observations
        ]:
        dtypes_map[col] = "Int64"

    return pd.read_csv(csv_path, dtype=dtypes_map)


def get_tess_ctois(cache_policy_func=DEFAULT_CACHE_POLICY):
    csv_path = download_utils.download_file(
        CTOI_CSV_URL,
        CTOI_CSV_LOCAL_FILENAME,
        download_dir=DOWNLOAD_DIR,
        cache_policy_func=cache_policy_func,
    )
    return pd.read_csv(csv_path, dtype={"CTOI": str, "Promoted to TOI": str})


def get_pht_ctois(cache_policy_func=DEFAULT_CACHE_POLICY):
    df = get_tess_ctois(cache_policy_func=cache_policy_func)
    return df[df["User"] == "eisner"].reset_index(drop=True)


def _get_coord_j2000_of_tics(tics):
    # For TIC table column description, see:
    # https://outerspace.stsci.edu/display/TESS/TIC+v8.2+and+CTL+v8.xx+Data+Release+Notes
    from astroquery.mast import Catalogs

    df = Catalogs.query_criteria(catalog="Tic", ID=tics, ).to_pandas()
    # Somehow MAST does not recognize `columns=["ID", "ra", "dec"]` parameter
    # so we do our filtering after the query
    df = df[["ID", "ra", "dec"]]
    df.rename(columns={"ID": "tic_id"}, inplace=True)
    df["tic_id"].astype('int64')
    return df


def _get_tess_points(tics, ra, dec):
    from tess_stars2px import tess_stars2px_function_entry

    # ra, dec in J2000
    # tics is actually ignored by the tess_stars2px
    # it is more for book keeping.
    (
        outID,
        outEclipLong,
        outEclipLat,
        outSec,
        outCam,
        outCcd,
        outColPix,
        outRowPix,
        scinfo,
    ) = tess_stars2px_function_entry(tics, ra, dec)

    df = pd.DataFrame(
        data=dict(
            tic_id=outID,
            sector=outSec,
            camera=outCam,
            ccd=outCcd,
            column=outColPix,
            row=outRowPix,
        )
    )
    return df



def download_pht_ctoi_sectors():
    csv_path = f"{DOWNLOAD_DIR}/{PHT_CTOI_SECTORS_CSV_LOCAL_FILENAME}"

    df_ctois = get_pht_ctois()
    df_coord = _get_coord_j2000_of_tics(df_ctois["TIC ID"])

    df_tesspoints = _get_tess_points(df_coord["tic_id"], df_coord["ra"], df_coord["dec"])

    # a compact form of tic_id,<list of sectors>
    df_sectors = (
        df_tesspoints[["tic_id", "sector"]]
        .groupby("tic_id")["sector"]
        # for the concatenated sector list, an ending "," is added to
        # make subsequent query easier
        .apply(lambda sectors: ",".join([str(s) for s in sectors]) + ",")
        .reset_index()
    )
    df_sectors.rename(columns={"sector": "sectors"}, inplace=True)

    df_sectors.to_csv(csv_path, index=False)
    return csv_path


def load_pht_ctoi_sectors():
    """Local the PHT CTOI sectors table from local store. It does not query remotely"""
    csv_path = f"{DOWNLOAD_DIR}/{PHT_CTOI_SECTORS_CSV_LOCAL_FILENAME}"
    return pd.read_csv(csv_path)


def create_pht_ctois_statuses_table(query_tesspoint=True, save=True, default_columns_only=True):
    csv_path = f"{DATA_DIR}/{PHT_CTOI_STATUSES_CSV_LOCAL_FILENAME}"

    df_ctois = get_pht_ctois()
    df_tois = get_tess_tois()
    if query_tesspoint:
        download_pht_ctoi_sectors()
    df_sectors = load_pht_ctoi_sectors()

    # Cannot use validate="one_to_one" because "Promoted to TOI" column has NaNs
    df = pd.merge(df_ctois, df_tois, how="left", left_on="Promoted to TOI", right_on="TOI", suffixes=[None, "_toi"])

    df = pd.merge(df, df_sectors, how="left", left_on="TIC ID", right_on="tic_id", suffixes=[None, "_wtv"], validate="many_to_one")

    # TODO: merge with PHT paper table

    # add convenience columns
    df["Transit Epoch (BTJD)"] = df["Transit Epoch (BJD)"] - BTJD_REF   # from CTOI

    # rename some columns to make them clearer
    df.rename(columns={
        "Notes": "CTOI Notes",
        "Comments": "TOI Comments",
        "Sectors": "TOI Sectors",  # to avoid confusion with the sectors from tess-point below
        "sectors": "WTV Sectors",
        "Master": "TOI Master Priority",
        "Date Modified": "TOI Date Modified",
    }, inplace=True)

    if default_columns_only:
        df = df [[
        "TIC ID", "CTOI", "TOI",
        "CTOI Notes", "TOI Comments",
        "WTV Sectors",
        "TFOPWG Disposition",
        "TOI Master Priority",
        "Time Series Observations", "Spectroscopy Observations", "Imaging Observations",
        "TESS Mag", "Transit Epoch (BTJD)", "Period (days)", "Depth ppm", "Duration (hrs)",
        "CTOI lastmod", "TOI Date Modified",
        ]]

    if save:
        df.to_csv(csv_path, index=False)
    return df


def load_pht_ctois_statuses_table():
    csv_path = f"{DATA_DIR}/{PHT_CTOI_STATUSES_CSV_LOCAL_FILENAME}"

    dtypes_map = { "TOI": str, "CTOI": str}

    # force nullable integer columns
    for col in [
        "TOI Master Priority",
        "Time Series Observations", "Spectroscopy Observations", "Imaging Observations",   # num. of observations
        ]:
        dtypes_map[col] = "Int64"

    return pd.read_csv(csv_path, dtype=dtypes_map)
