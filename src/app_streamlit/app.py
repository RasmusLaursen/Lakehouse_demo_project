import os
import io
import toml
import streamlit as st
from databricks.sdk import WorkspaceClient
import datetime
import uuid
from databricks.sdk.core import Config
from databricks import sql
import pandas as pd

config_path = "config.toml"
if not os.path.exists(config_path):
    st.error("Configuration file not found. Please create a config.toml file.")
    st.stop()

config = toml.load(config_path)

def table_exists(table_name: str) -> bool:
    try:
        query = f"SHOW TABLES LIKE '{table_name}'"
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().num_rows > 0
    except Exception as e:
        st.error(f"Error checking table existence: {e}", icon="🚨")
        return False
    
def create_table_if_not_exists(table_name: str, conn):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        guid STRING,
        name STRING,
        email STRING,
        from_date DATE,
        to_date DATE,
        booking_time TIMESTAMP,
        notes STRING,
        confirmation_date TIMESTAMP,
        is_confirmed BOOLEAN
    )
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)    
     
def check_upload_permissions(volume_name: str):
    try:
        volume = w.volumes.read(name=volume_name)
        current_user = w.current_user.me()
        grants = w.grants.get_effective(
            securable_type="volume",
            full_name=volume.full_name,
            principal=current_user.user_name,
        )

        if not grants or not grants.privilege_assignments:
            return "Insufficient permissions: No grants found."

        for assignment in grants.privilege_assignments:
            for privilege in assignment.privileges:
                if privilege.privilege.value in ["ALL_PRIVILEGES", "WRITE_VOLUME"]:
                    return "Volume and permissions validated"

        return "Insufficient permissions: Required privileges not found."
    except Exception as e:
        return f"Error: {e}"


if "volume_check_success" not in st.session_state:
    st.session_state.volume_check_success = False

databricks_host = os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_HOSTNAME")
w = WorkspaceClient()

st.header(body="Bookings", divider=True)
# st.subheader(f"{config["development"]["app"]["header"]}")

tab1, tab2 = st.tabs(["**Create booking**", "**Confirm Bookings**"])

with tab1:
    st.subheader("Booking Information")

    booking_name = st.text_input("Name")
    booking_email = st.text_input("Email")
    lakehouse_options = [    "Silver Fjord Retreat",
    "Nordlys Cabin",
    "Skovsø Lodge",
    "Fjeldhavn Hideaway",
    "Søhjerte Chalet",
    "Himmelbryn Cottage"]
    selected_lakehouse = st.selectbox("Select a Lakehouse", lakehouse_options)

    from_date = st.date_input("From Date")
    to_date = st.date_input("To Date", min_value=from_date)

    if to_date < from_date:
        st.warning("To Date must be after From Date.", icon="⚠️")

    booking_time = booking_time = datetime.datetime.now().time()
    
    booking_notes = st.text_area("Additional Notes")

    booking_body = {
        "guid": str(uuid.uuid4()),
        "name": booking_name,
        "email": booking_email,
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "booking_time": booking_time.isoformat(),
        "notes": booking_notes,
    }

    if st.button("Submit Booking"):
        if not booking_name or not booking_email:
            st.warning("Please fill out all required fields.", icon="⚠️")
        else:
            permission_result = check_upload_permissions(f"fast_api_source.bookings.bookings")
            if permission_result == "Volume and permissions validated":
                st.session_state.volume_check_success = True
                # st.success("Volume and permissions validated", icon="✅")
            else:
                st.session_state.volume_check_success = False
                st.error(permission_result, icon="🚨")

            if st.session_state.volume_check_success:
                try:
                    landing_catalog = config["development"]["app"]["catalog"]
                    landing_schema = config["development"]["app"]["schema"]
                    binary_data = io.BytesIO(str(booking_body).encode('utf-8'))  # Convert dict to bytes
                    volume_file_path = f"/Volumes/{landing_catalog}/{landing_schema}/bookings/{booking_name}_{booking_time.isoformat()}_{booking_time.isoformat()}.json"
                    w.files.upload(volume_file_path, binary_data, overwrite=True)
                except Exception as e:
                    st.error(f"Error writing booking: {e}", icon="🚨"                             )
            st.success("Booking submitted successfully!", icon="✅")


cfg = Config()  # Set the DATABRICKS_HOST environment variable when running locally

with tab2:
    @st.cache_resource # connection is cached
    def get_connection(http_path):
        return sql.connect(
            server_hostname=cfg.host,
            http_path=http_path,
            credentials_provider=lambda: cfg.authenticate,
        )

    def read_table(table_name, table_name_confirmations, conn):
        with conn.cursor() as cursor:
            query = f"""
            SELECT * 
            FROM {table_name} 
            WHERE guid NOT IN (SELECT guid FROM {table_name_confirmations})
            """
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()
        
    def get_warehouse_id(name:str):
        warehouses = w.warehouses.list()
        for warehouse in warehouses:
            if warehouse.name == name:
                return warehouse.id
        return None

    table_name = "base_dev.dev_rahl_bookings.bookings"
    table_name_confirmations = "base_dev.dev_rahl_bookings.bookings_test"

    warehouse_id = get_warehouse_id("Serverless Starter Warehouse")

    if warehouse_id is None:
        st.error("Warehouse not found.", icon="🚨")

    def get_data(conn):
        try:
            st.session_state.df = read_table(table_name, table_name_confirmations, conn)
            
            # df = read_table(table_name, table_name_confirmations, conn)
            # selected_rows = st.dataframe(st.session_state.df, hide_index=True, selection_mode="multi-row", on_select="rerun")
        except Exception as e:
            st.error(f"Error retrieving data: {e}", icon="🚨")

    http_path_input = f"/sql/1.0/warehouses/{warehouse_id}"
    conn = get_connection(http_path_input)

    with st.spinner("Loading booking data..."):
        get_data(conn=conn)


    # with st.spinner("Confirming booking data..."):
    #     try:
    #         def save_data():
    #             if selected_rows:
    #                 confirmed_booking = st.session_state.df.iloc[selected_rows["selection"]["rows"]]

    #                 # Append confirmed_booking to Delta table
    #                 try:
    #                     # Convert confirmed_booking DataFrame to a list of dictionaries
    #                     booking_data = confirmed_booking[[
    #                         'guid', 'name', 'email', 'from_date', 
    #                         'to_date', 'booking_time', 'notes'
    #                     ]].to_dict(orient='records')

    #                     for booking in booking_data:
    #                         booking['confirmation_date'] = datetime.datetime.now().isoformat()
    #                         booking['is_confirmed'] = True

    #                     table_bool = table_exists(table_name=table_name_confirmations)

    #                     if not table_bool:
    #                         create_table_if_not_exists(table_name_confirmations, conn)

    #                     with conn.cursor() as cursor:
    #                         for booking in booking_data:
    #                             cursor.execute(f"INSERT INTO {table_name_confirmations} VALUES {tuple(booking.values())}")
                            
    #                     # Remove confirmed bookings from the displayed DataFrame
    #                     st.session_state.df = st.session_state.df[~st.session_state.df['guid'].isin(confirmed_booking['guid'])]
    #                     # st.dataframe(st.session_state.df)
    #                     st.success("Booking confirmed and added to Delta table!", icon="✅")
    #                 except Exception as e:
    #                     st.error(f"Error appending booking to Delta table: {e}", icon="🚨")

    #                 # st.write(f"Confirmed Booking Details:{confirmed_booking}" )
    #             else:
    #                 st.warning("Please select a booking to confirm.", icon="⚠️")

    #         st.button("Confirm Booking", on_click=save_data)
    #     except Exception as e:
    #         st.error(f"Error saving booking data: {e}", icon="🚨")            

    def save_data(conn):
        try:
            # Convert confirmed_booking DataFrame to a list of dictionaries
            booking_data = st.session_state.confirmed_booking[[
                'guid', 'name', 'email', 'from_date', 
                'to_date', 'booking_time', 'notes'
            ]].to_dict(orient='records')

            for booking in booking_data:
                booking['confirmation_date'] = datetime.datetime.now().isoformat()
                booking['is_confirmed'] = True

            table_bool = table_exists(table_name=table_name_confirmations)

            if not table_bool:
                create_table_if_not_exists(table_name_confirmations, conn)

            with conn.cursor() as cursor:
                for booking in booking_data:
                    cursor.execute(f"INSERT INTO {table_name_confirmations} VALUES {tuple(booking.values())}")

            # Remove confirmed bookings from the displayed DataFrame
            st.session_state.df = st.session_state.df[~st.session_state.df['guid'].isin(st.session_state.confirmed_booking['guid'])]
            # st.success("Booking confirmed and added to Delta table!", icon="✅")
        except Exception as e:
            st.error(f"Error appending booking to Delta table: {e}", icon="🚨")

    # Main app logic
    try:
        if "df" in st.session_state and not st.session_state.df.empty:
            selected_rows = st.dataframe(st.session_state.df, hide_index=True, selection_mode="multi-row", on_select="rerun")
            if selected_rows["selection"]["rows"]:
                st.session_state.confirmed_booking = st.session_state.df.iloc[selected_rows["selection"]["rows"]]
                st.button("Confirm Booking", on_click=save_data(conn=conn))
            else:
                st.warning("Please select a booking to confirm.", icon="⚠️")
        else:
            st.warning("No bookings available to confirm.", icon="⚠️")
            st.button("Refresh Data", on_click=get_data(conn=conn))
    except Exception as e:
        st.error(f"Error saving booking data: {e}", icon="🚨")