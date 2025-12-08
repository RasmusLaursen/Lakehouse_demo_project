from open_data_contract_standard.model import OpenDataContractStandard, SchemaObject
from faker import Faker
import random
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
import uuid
from pathlib import Path
from src.helper import logging_helper
from src.helper import data_contract_helper

logger = logging_helper.get_logger(__name__)

class DynamicFakeDataGenerator:
    """Generate fake data dynamically based on ODCS data contract specifications."""
    
    def __init__(self, data_contract_file: str, locale: str = 'en_US'):
        """
        Initialize the fake data generator.
        
        Args:
            data_contract_file: Path to the data contract YAML file
            locale: Faker locale for generating localized data
        """
        self.faker = Faker(locale)
        self.data_contract = data_contract_helper.load_data_contract(Path(data_contract_file))
        self.schemas = data_contract_helper.get_all_schemas(self.data_contract)
        self.generated_ids = {}  # Track generated IDs for foreign keys
        
    def generate_value_for_field(self, field_name: str, field_spec: Any) -> Any:
        """Generate a fake value based on field specification."""
        field_type = getattr(field_spec, 'type', 'string').lower()
        
        # Handle different data types
        if field_type == 'integer':
            return self._generate_integer(field_name, field_spec)
        elif field_type == 'string':
            return self._generate_string(field_name, field_spec)
        elif field_type == 'boolean':
            return self._generate_boolean(field_name, field_spec)
        elif field_type == 'date':
            return self._generate_date(field_name, field_spec)
        elif field_type == 'datetime' or field_type == 'timestamp':
            return self._generate_datetime(field_name, field_spec)
        elif field_type in ['float', 'decimal', 'number']:
            return self._generate_float(field_name, field_spec)
        elif field_type == 'array':
            return self._generate_array(field_name, field_spec)
        else:
            return self._generate_string(field_name, field_spec)
    
    def _generate_integer(self, field_name: str, field_spec: Any) -> int:
        """Generate integer value based on constraints."""
        min_val = getattr(field_spec, 'minimum', 1)
        max_val = getattr(field_spec, 'maximum', 1000000)

        if min_val is None:
            min_val = 1
        if max_val is None:
            max_val = 1000000
        
        # Handle primary keys - ensure uniqueness
        if getattr(field_spec, 'primaryKey', False) or '_id' in field_name.lower():
            if field_name not in self.generated_ids:
                self.generated_ids[field_name] = set()
            
            attempts = 0
            while attempts < 100:  # Prevent infinite loops
                value = random.randint(min_val, max_val)
                if value not in self.generated_ids[field_name]:
                    self.generated_ids[field_name].add(value)
                    return value
                attempts += 1
            
            # Fallback: use a unique value
            value = max(self.generated_ids[field_name]) + 1 if self.generated_ids[field_name] else min_val
            self.generated_ids[field_name].add(value)
            return value
        
        # Handle foreign keys
        foreign_key = getattr(field_spec, 'foreignKey', None)
        if foreign_key:
            # Extract referenced field name
            ref_field = foreign_key.split('.')[-1]
            if ref_field in self.generated_ids and self.generated_ids[ref_field]:
                return random.choice(list(self.generated_ids[ref_field]))
        
        return random.randint(min_val, max_val)
    
    def _generate_string(self, field_name: str, field_spec: Any) -> str:
        """Generate string value based on constraints and field name patterns."""
        min_length = getattr(field_spec, 'minLength', 1)
        max_length = getattr(field_spec, 'maxLength', 50)
        pattern = getattr(field_spec, 'pattern', None)
        enum_values = getattr(field_spec, 'enum', None)
        format_type = getattr(field_spec, 'format', None)

        if min_length is None:
            min_length = 1
        if max_length is None:
            max_length = 50
        
        # Handle enum values
        if enum_values:
            return random.choice(enum_values)
        
        # Handle specific formats
        if format_type == 'email':
            return self.faker.email()
        elif format_type == 'uuid':
            return str(uuid.uuid4())
        
        # Handle field name patterns
        field_lower = field_name.lower()
        
        if 'email' in field_lower:
            return self.faker.email()
        elif 'phone' in field_lower or 'telephone' in field_lower:
            return self.faker.phone_number()
        elif 'name' in field_lower and 'first' in field_lower:
            return self.faker.first_name()
        elif 'name' in field_lower and 'last' in field_lower:
            return self.faker.last_name()
        elif 'name' in field_lower:
            return self.faker.name()
        elif 'address' in field_lower:
            return self.faker.address().replace('\n', ', ')
        elif 'city' in field_lower:
            return self.faker.city()
        elif 'country' in field_lower:
            return self.faker.country_code() if max_length <= 2 else self.faker.country()
        elif 'postal' in field_lower or 'zip' in field_lower:
            return self.faker.postcode()
        elif 'company' in field_lower:
            return self.faker.company()
        elif 'description' in field_lower:
            return self.faker.text(max_nb_chars=min(max_length, 200))
        elif 'transaction' in field_lower and 'id' in field_lower:
            return self.faker.uuid4()
        elif 'currency' in field_lower:
            return random.choice(['USD', 'EUR', 'GBP', 'CAD'])
        
        # Handle patterns
        if pattern:
            try:
                return self.faker.bothify(text=self._convert_regex_to_bothify(pattern))
            except:
                pass
        
        # Default string generation
        if max_length <= 10:
            return self.faker.word()[:max_length]
        elif max_length <= 50:
            return self.faker.sentence(nb_words=random.randint(2, 5))[:max_length]
        else:
            return self.faker.text(max_nb_chars=max_length)
    
    def _generate_boolean(self, field_name: str, field_spec: Any) -> bool:
        """Generate boolean value."""
        # Check for default value
        default = getattr(field_spec, 'default', None)
        if default is not None:
            return default if random.random() > 0.3 else not default  # 70% chance of default
        
        # Field name based logic
        field_lower = field_name.lower()
        if 'is_active' in field_lower or 'active' in field_lower:
            return random.choices([True, False], weights=[0.8, 0.2])[0]  # 80% active
        elif 'is_subscribed' in field_lower:
            return random.choices([True, False], weights=[0.6, 0.4])[0]  # 60% subscribed
        elif 'has_' in field_lower:
            return random.choices([True, False], weights=[0.7, 0.3])[0]  # 70% has feature
        
        return self.faker.boolean()
    
    def _generate_date(self, field_name: str, field_spec: Any) -> date:
        """Generate date value based on constraints."""
        field_lower = field_name.lower()
        
        if 'birth' in field_lower:
            return self.faker.date_of_birth(minimum_age=18, maximum_age=80)
        elif 'hire' in field_lower or 'start' in field_lower:
            return self.faker.date_between(start_date='-5y', end_date='today')
        elif 'check_in' in field_lower:
            return self.faker.date_between(start_date='today', end_date='+30d')
        elif 'check_out' in field_lower:
            # This should be handled in relation to check_in_date
            return self.faker.date_between(start_date='+1d', end_date='+31d')
        elif 'last_' in field_lower or 'recent' in field_lower:
            return self.faker.date_between(start_date='-1y', end_date='today')
        elif 'listing' in field_lower:
            return self.faker.date_between(start_date='-2y', end_date='-30d')
        
        # Handle minimum/maximum constraints
        min_date = getattr(field_spec, 'minimum', None)
        max_date = getattr(field_spec, 'maximum', None)
        
        if min_date and max_date:
            return self.faker.date_between(start_date=min_date, end_date=max_date)
        elif min_date:
            return self.faker.date_between(start_date=min_date, end_date='today')
        elif max_date:
            return self.faker.date_between(start_date='-10y', end_date=max_date)
        
        return self.faker.date_between(start_date='-2y', end_date='+1y')
    
    def _generate_datetime(self, field_name: str, field_spec: Any) -> datetime:
        """Generate datetime value."""
        date_val = self._generate_date(field_name, field_spec)
        time_val = datetime.strptime(self.faker.time(), "%H:%M:%S").time()
        return datetime.combine(date_val, time_val)
    
    def _generate_float(self, field_name: str, field_spec: Any) -> float:
        """Generate float value based on constraints."""
        min_val = getattr(field_spec, 'minimum', 0.0)
        max_val = getattr(field_spec, 'maximum', 10000.0)

        if min_val is None:
            min_val = 0.0
        if max_val is None:
            max_val = 10000.0
        
        field_lower = field_name.lower()
        
        if 'rate' in field_lower and 'nightly' in field_lower:
            return round(random.uniform(50.0, 2000.0), 2)
        elif 'cost' in field_lower or 'price' in field_lower or 'spend' in field_lower:
            return round(random.uniform(min_val, min(max_val, 50000.0)), 2)
        elif 'rating' in field_lower:
            return round(random.uniform(1.0, 5.0), 1)
        elif 'commission' in field_lower and 'rate' in field_lower:
            return round(random.uniform(0.02, 0.15), 3)  # 2-15%
        elif 'tax' in field_lower:
            return round(random.uniform(0.0, 500.0), 2)
        
        return round(random.uniform(min_val, max_val), 2)
    
    def _generate_array(self, field_name: str, field_spec: Any) -> List[Any]:
        """Generate array value."""
        field_lower = field_name.lower()
        
        if 'amenities' in field_lower:
            amenities = ['WiFi', 'Kitchen', 'Parking', 'Pool', 'Gym', 'Spa', 'Restaurant', 
                        'Bar', 'Concierge', 'Laundry', 'Pet Friendly', 'Business Center']
            return random.sample(amenities, k=random.randint(2, 6))
        elif 'assigned' in field_lower and 'lakehouse' in field_lower:
            return [random.randint(1, 100) for _ in range(random.randint(1, 5))]
        
        # Default array generation
        return [self.faker.word() for _ in range(random.randint(1, 5))]
    
    def _convert_regex_to_bothify(self, pattern: str) -> str:
        """Convert regex pattern to Faker bothify format (simplified)."""
        # This is a simplified conversion - you might need to expand this
        pattern = pattern.replace(r'\d', '#')
        pattern = pattern.replace(r'[A-Z]', '?')
        pattern = pattern.replace(r'[a-z]', '?')
        return pattern
    
    def generate_record(self, model_name: str, relationships: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Generate a single fake record for the specified model.
        
        Args:
            model_name: Name of the model to generate data for
            relationships: Pre-existing relationship values (e.g., foreign keys)
        
        Returns:
            Dictionary containing the generated fake data
        """
        # Find the schema by name
        schema = None
        for s in self.schemas:
            if s.name == model_name:
                schema = s
                break
        
        if schema is None:
            raise ValueError(f"Model '{model_name}' not found in data contract")
        
        record = {}
        
        # Handle relationships first
        if relationships:
            record.update(relationships)
        
        # Generate values for each field
        if schema.properties:
            for field_spec in schema.properties:
                field_name = field_spec.name
                if not field_name or field_name in record:  # Skip if no name or already provided
                    continue
                    
                record[field_name] = self.generate_value_for_field(field_name, field_spec)
        
        # Post-process for business logic
        record = self._apply_business_logic(model_name, record)
        
        return record
    
    def _apply_business_logic(self, model_name: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply business logic constraints and relationships."""
        
        # Lakehouse rental specific logic
        if model_name == 'lakehouse_rental':
            # Ensure check_out_date is after check_in_date
            if 'check_in_date' in record and 'check_out_date' in record:
                check_in = record['check_in_date']
                if isinstance(check_in, str):
                    check_in = datetime.strptime(check_in, '%Y-%m-%d').date()
                
                # Generate check_out_date 1-7 days after check_in
                days_stay = random.randint(1, 7)
                record['check_out_date'] = check_in + timedelta(days=days_stay)
            
            # Calculate total_cost based on nightly_rate and stay duration
            if 'nightly_rate' in record and 'check_in_date' in record and 'check_out_date' in record:
                check_in = record['check_in_date']
                check_out = record['check_out_date']
                
                if isinstance(check_in, str):
                    check_in = datetime.strptime(check_in, '%Y-%m-%d').date()
                if isinstance(check_out, str):
                    check_out = datetime.strptime(check_out, '%Y-%m-%d').date()
                
                nights = (check_out - check_in).days
                record['total_cost'] = round(record['nightly_rate'] * nights, 2)
            
            # Calculate tax and total with tax
            if 'total_cost' in record:
                tax_rate = random.uniform(0.05, 0.15)  # 5-15% tax
                record['tax_amount'] = round(record['total_cost'] * tax_rate, 2)
                record['total_cost_with_tax'] = record['total_cost'] + record['tax_amount']
        
        return record
    
    def generate_dataset(self, model_name: str, count: int, 
                        related_data: Optional[Dict[str, List[Dict]]] = None) -> List[Dict[str, Any]]:
        """
        Generate multiple fake records for the specified model.
        
        Args:
            model_name: Name of the model to generate data for
            count: Number of records to generate
            related_data: Dictionary of related model data for foreign key relationships
        
        Returns:
            List of dictionaries containing the generated fake data
        """
        records = []
        
        for i in range(count):
            relationships = {}
            
            # Handle foreign key relationships
            if related_data:
                # Find the schema
                schema = None
                for s in self.schemas:
                    if s.name == model_name:
                        schema = s
                        break
                
                if schema and schema.properties:
                    for field_spec in schema.properties:
                        foreign_key = getattr(field_spec, 'foreignKey', None)
                        if foreign_key and '.' in foreign_key:
                            table_name, key_field = foreign_key.split('.')
                            if table_name in related_data and related_data[table_name]:
                                related_record = random.choice(related_data[table_name])
                                relationships[field_spec.name] = related_record.get(key_field)
            
            record = self.generate_record(model_name, relationships)
            records.append(record)
        
        return records

    def generate_all_models(self, counts: Optional[Dict[str, int]] = None) -> Dict[str, List[Dict]]:
        """
        Generate fake data for all models in the data contract.
        
        Args:
            counts: Dictionary specifying how many records to generate for each model
        
        Returns:
            Dictionary with model names as keys and lists of generated records as values
        """
        if counts is None:
            counts = {schema.name: 10 for schema in self.schemas if schema.name}
        
        all_data = {}
        
        # Generate reference data first (models without foreign keys)
        reference_models = []
        dependent_models = []
        
        for schema in self.schemas:
            model_name = schema.name
            if not model_name:
                continue
                
            has_foreign_keys = False
            if schema.properties:
                has_foreign_keys = any(
                    getattr(field_spec, 'foreignKey', None) 
                    for field_spec in schema.properties
                )
            
            if has_foreign_keys:
                dependent_models.append(model_name)
            else:
                reference_models.append(model_name)
        
        # Generate reference data first
        for model_name in reference_models:
            count = counts.get(model_name, 10)
            all_data[model_name] = self.generate_dataset(model_name, count)
        
        # Generate dependent data
        for model_name in dependent_models:
            count = counts.get(model_name, 10)
            all_data[model_name] = self.generate_dataset(model_name, count, all_data)
        
        return all_data