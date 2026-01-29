transform_config = {
    "uppercase": ["customer_name"],
    "filter": "status == 'ACTIVE'",
    "joins": [
        {
            "df_name": "department_df",
            "on": ["dept_id"],
            "how": "left"
        },
        {
            "df_name": "location_df",
            "left_on": "location_id",
            "right_on": "loc_id",
            "how": "inner",
            "suffixes": ("", "_loc")
        }
    ],
    "select": [
        "customer_id",
        "customer_name",
        "dept_name",
        "city",
        "salary"
    ]
}
