# from open_data_contract_standard.model import OpenDataContractStandard
# from pathlib import Path

# catalog = "source_system"

# # Define source system name
# source_system_name = "dqx_contract_v3"

# contract_path = "src\data_contracts\source_system\dqx_contract_v3.yml"

# data_configuration_path = Path(f"../data_contracts/{catalog}/{source_system_name}.yml")

# print(data_configuration_path.cwd())


# tset ="C:\\Users\\RasmusHolmLaursen\\OneDrive - twoday\\Desktop\\github\\Lakehouse_demo_project\\src\\data_contracts\\source_system\\dqx_contract_v3.yml"

# data_contract = OpenDataContractStandard.from_file(tset)

# from open_data_contract_standard.model import SchemaObject
# from open_data_contract_standard.model import CustomProperty

# tables_to_ingest: list[SchemaObject] = data_contract.schema_ # type: ignore

# for schema in tables_to_ingest:
#     print(schema.name)

#     custom_properties: list[CustomProperty] = schema.customProperties # type: ignore
#     columns: list[SchemaObject] = schema.properties # type: ignore

#     for custom_property in custom_properties:
#         print(f"  Property: {custom_property.property} - Value: {custom_property.value}")


from src.helper import data_contract_helper

# Define source system name
source_system_name = "dqx_contract_v3"


data_contract_specification = data_contract_helper.get_data_contract(
    catalog="source_system", object_name=source_system_name
)

print(data_contract_specification.servers)

