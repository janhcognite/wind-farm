
def get_ext_id(source_ext_id, turbine_no):
    ts_ext_id = "WT" + str(turbine_no).zfill(2) + source_ext_id.replace("-WindTurbine", "").replace("V52", "")

    return ts_ext_id
