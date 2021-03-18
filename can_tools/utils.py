def determine_location_column(df):
    return "location" if "location" in df.columns else "location_name"
