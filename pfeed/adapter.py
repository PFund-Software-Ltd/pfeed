from typing import Union, Any, get_origin, get_args
from abc import ABC, abstractmethod
import datetime
from decimal import Decimal
import types  # For UnionType detection

import pyarrow as pa
from pydantic import BaseModel
from msgspec import Struct


class TypeMapper:
    """
    Maps Python types to PyArrow data types.
    
    Handles primitive types, collections, and nested model conversion.
    Supports custom field-specific mappings for special cases.
    """
    
    def __init__(self, custom_mappings: dict[str, pa.DataType] | None = None):
        """
        Initialize type mapper with optional custom field mappings.
        
        Args:
            custom_mappings: Dict mapping field names to PyArrow types
        """
        self.custom_mappings = custom_mappings or {}
        
        # Core Python type mappings
        self.type_map = {
            int: pa.int64(),
            float: pa.float64(), 
            str: pa.string(),
            bool: pa.bool_(),
            bytes: pa.binary(),
            datetime.datetime: pa.timestamp('us'),
            datetime.date: pa.date32(),
            Decimal: pa.decimal128(38, 10),
            # Raw collections with default element types
            list: pa.list_(pa.string()),  # Explicit typing preferred: list[int], list[str], etc.
            dict: pa.map_(pa.string(), pa.string())  # Explicit typing preferred: dict[str, int], etc.
        }

    def python_type_to_pyarrow(self, python_type: Any, field_name: str | None = None, value: Any = None) -> pa.DataType:
        """
        Convert a Python type to PyArrow DataType.
        
        Args:
            python_type: The Python type to convert (can be type, Union, etc.)
            field_name: Optional field name for custom mappings
            value: Optional actual value to help with type inference (for concrete instances)
            
        Returns:
            PyArrow DataType
        """
        # Check custom mappings first
        if field_name and field_name in self.custom_mappings:
            return self.custom_mappings[field_name]
        
        # Handle Union types (both legacy typing.Union and modern X | Y syntax)
        origin = get_origin(python_type)
        if origin is Union or (hasattr(types, 'UnionType') and origin is types.UnionType):
            args = get_args(python_type)
            # Handle Optional[T] which is Union[T, None] or T | None
            if len(args) == 2 and type(None) in args:
                non_none_type = args[0] if args[1] is type(None) else args[1]
                return self.python_type_to_pyarrow(non_none_type, field_name, value)
            # Handle Union of multiple types - default to string
            return pa.string()
        
        # Handle List types - get_origin works for both list and List from typing
        if origin is list:
            args = get_args(python_type)
            if args:
                item_type = self.python_type_to_pyarrow(args[0])
                return pa.list_(item_type)
            return pa.list_(pa.string())
        
        # Handle Dict types - get_origin works for both dict and Dict from typing
        if origin is dict:
            args = get_args(python_type)
            if len(args) >= 2:
                key_type = self.python_type_to_pyarrow(args[0])
                value_type = self.python_type_to_pyarrow(args[1])
                return pa.map_(key_type, value_type)
            return pa.map_(pa.string(), pa.string())
        
        # 🚀 SMART: Handle concrete dict instances with mixed types as structs
        if python_type is dict and isinstance(value, dict) and value:
            return self._convert_dict_to_struct(value)
        
        # 🚀 SMART: Handle concrete list instances by inspecting the first element
        if python_type is list and isinstance(value, list) and value:
            # Inspect the first element to determine the element type
            first_element = value[0]
            element_type = self.python_type_to_pyarrow(type(first_element), None, first_element)
            return pa.list_(element_type)
        
        # Handle nested Pydantic/Msgspec models as structs
        if self._is_nested_model(python_type):
            return self._convert_model_to_struct(python_type)
        
        # Handle basic types (including raw list/dict)
        if python_type in self.type_map:
            return self.type_map[python_type]
        
        # Default fallback for unknown types
        return pa.string()
    
    def _convert_dict_to_struct(self, dict_value: dict) -> pa.DataType:
        """
        Convert a concrete dict instance to PyArrow struct type.
        
        This preserves individual field types instead of forcing all values to the same type.
        Used for mixed-type dicts like extra_data with int, str, bool values.
        """
        struct_fields = []
        
        for key, val in dict_value.items():
            if not isinstance(key, str):
                # Non-string keys not supported in struct, fallback to map
                return pa.map_(pa.string(), pa.string())
            
            if val is None:
                # For None values, default to string (could be improved with heuristics)
                val_type = pa.string()
            else:
                # Recursively handle nested values
                val_type = self.python_type_to_pyarrow(type(val), key, val)
            
            struct_fields.append(pa.field(key, val_type, nullable=True))
        
        return pa.struct(struct_fields)
    
    def _is_nested_model(self, python_type: Any) -> bool:
        """Check if the type is a nested Pydantic or Msgspec model."""
        try:
            # Check if it's a class and if it's a Pydantic or Msgspec model
            return (
                isinstance(python_type, type) and (
                    issubclass(python_type, BaseModel) or 
                    issubclass(python_type, Struct)
                )
            )
        except TypeError:
            return False
    
    def _convert_model_to_struct(self, model_class: Any) -> pa.DataType:
        """Convert a nested Pydantic/Msgspec model to PyArrow struct type."""
        struct_fields = []
        
        # Use the same inspection logic as the main adapter
        if issubclass(model_class, BaseModel):
            # Pydantic model
            model_fields = model_class.model_fields
            for field_name, field_def in model_fields.items():
                field_type = field_def.annotation
                is_required = field_def.is_required()
                
                pyarrow_type = self.python_type_to_pyarrow(field_type, field_name)
                struct_field = pa.field(field_name, pyarrow_type, nullable=not is_required)
                struct_fields.append(struct_field)
                
        elif issubclass(model_class, Struct):
            # Msgspec struct
            if hasattr(model_class, '__struct_fields__'):
                for field_name in model_class.__struct_fields__:
                    # Get type annotation
                    annotations = getattr(model_class, '__annotations__', {})
                    field_type = annotations.get(field_name, str)
                    
                    # Determine if nullable based on type annotation
                    origin = get_origin(field_type)
                    is_nullable = origin is Union and type(None) in get_args(field_type)
                    
                    pyarrow_type = self.python_type_to_pyarrow(field_type, field_name)
                    struct_field = pa.field(field_name, pyarrow_type, nullable=is_nullable)
                    struct_fields.append(struct_field)
        
        return pa.struct(struct_fields)
    
    def add_custom_mapping(self, field_name: str, pyarrow_type: pa.DataType):
        """Add or update a custom field mapping."""
        self.custom_mappings[field_name] = pyarrow_type


class ModelInspector(ABC):
    """
    Abstract base class for inspecting data models (pydantic/msgspec).
    """
    
    @abstractmethod
    def get_field_info(self, model_class: Any) -> dict[str, dict[str, Any]]:
        """
        Extract field information from a model class.
        
        Args:
            model_class: The model class to inspect
            
        Returns:
            Dict mapping field names to field info:
            {
                'field_name': {
                    'type': <python_type>,
                    'nullable': <bool>,
                    'default': <Any>,
                    'annotation': <Any>
                }
            }
        """
        raise NotImplementedError
    
    @abstractmethod
    def supports_model(self, model_class: Any) -> bool:
        """Check if this inspector can handle the given model class."""
        raise NotImplementedError


class PydanticInspector(ModelInspector):
    """Inspector for Pydantic models."""
    
    def get_field_info(self, model_class: Any) -> dict[str, dict[str, Any]]:
        """Extract field information from a Pydantic model."""
        if not self.supports_model(model_class):
            raise ValueError(f"{model_class} is not a Pydantic model")
        
        field_info = {}
        model_fields = model_class.model_fields
        
        for field_name, field_def in model_fields.items():
            field_type = field_def.annotation
            is_required = field_def.is_required()
            default_value = field_def.default if hasattr(field_def, 'default') else None
            
            field_info[field_name] = {
                'type': field_type,
                'nullable': not is_required,
                'default': default_value,
                'annotation': field_type,
            }
        
        return field_info
    
    def supports_model(self, model_class: Any) -> bool:
        """Check if this is a Pydantic model."""
        return issubclass(model_class, BaseModel)


class MsgspecInspector(ModelInspector):
    """Inspector for Msgspec structs."""
    
    def get_field_info(self, model_class: Any) -> dict[str, dict[str, Any]]:
        """Extract field information from a Msgspec struct."""
        if not self.supports_model(model_class):
            raise ValueError(f"{model_class} is not a Msgspec struct")
        
        field_info = {}
        
        # Get field information from __struct_fields__
        if hasattr(model_class, '__struct_fields__'):
            for field_name in model_class.__struct_fields__:
                # Get type annotation
                annotations = getattr(model_class, '__annotations__', {})
                field_type = annotations.get(field_name, str)
                
                # Check if field has default value
                default_value = getattr(model_class, field_name, None) if hasattr(model_class, field_name) else None
                
                # Determine if nullable based on type annotation
                origin = get_origin(field_type)
                is_nullable = origin is Union and type(None) in get_args(field_type)
                
                field_info[field_name] = {
                    'type': field_type,
                    'nullable': is_nullable,
                    'default': default_value,
                    'annotation': field_type,
                }
        
        return field_info
    
    def supports_model(self, model_class: Any) -> bool:
        """Check if this is a Msgspec struct."""
        return issubclass(model_class, Struct)


class Adapter:
    """
    Adapter to convert pydantic/msgspec models to pyarrow schema.
    
    Main entry point for schema conversion. Handles model inspection,
    type mapping, and schema generation.
    """
    
    def __init__(self, custom_field_mappings: dict[str, pa.DataType] | None = None):
        """
        Initialize the adapter.
        
        Args:
            custom_field_mappings: Optional dict mapping specific field names 
                                 to PyArrow types for custom handling
        """
        self.type_mapper = TypeMapper(custom_field_mappings)
        self.inspectors = [
            PydanticInspector(),
            MsgspecInspector(),
        ]
    
    def model_to_schema(self, model_class: Any, include_fields: list | None = None, 
                       exclude_fields: list | None = None) -> pa.Schema:
        """
        Convert a pydantic/msgspec model to PyArrow schema.
        
        Args:
            model_class: The model class to convert, not instance
            include_fields: Only include these fields (None = include all)
            exclude_fields: Exclude these fields (None = exclude none)
            
        Returns:
            PyArrow Schema
            
        Raises:
            ValueError: If model type is not supported
        """
        inspector = self._get_inspector_for_model(model_class)
        field_info = inspector.get_field_info(model_class)
        
        # Apply field filtering
        if include_fields:
            field_info = {k: v for k, v in field_info.items() if k in include_fields}
        if exclude_fields:
            field_info = {k: v for k, v in field_info.items() if k not in exclude_fields}
        
        return self._build_schema_from_fields(field_info)
    
    def instance_to_schema(self, model_instance: Any, **kwargs) -> pa.Schema:
        """
        Convert a model instance to PyArrow schema.
        Convenience method that extracts the class from an instance.
        
        Args:
            model_instance: Instance of pydantic/msgspec model
            **kwargs: Passed to model_to_schema
            
        Returns:
            PyArrow Schema
        """
        return self.model_to_schema(type(model_instance), **kwargs)
    
    def dict_to_schema(self, data_dict: dict) -> pa.Schema:
        """
        Create PyArrow schema from a dictionary's field names and value types.
        
        Args:
            data_dict: Dictionary with field names as keys and values to infer types from
            
        Returns:
            PyArrow Schema with fields based on dictionary contents
        """
        fields = []
        
        for field_name, value in data_dict.items():
            # Get the appropriate PyArrow type for this field and value
            if value is None:
                # For None values, use custom mappings or default to string
                pyarrow_type = self.type_mapper.python_type_to_pyarrow(str, field_name, value)
            else:
                # 🚀 PASS VALUE: Now we pass the actual value so dict instances can be converted to structs
                pyarrow_type = self.type_mapper.python_type_to_pyarrow(type(value), field_name, value)
            
            # All streaming fields should be nullable (to handle None values gracefully)
            field = pa.field(field_name, pyarrow_type, nullable=True)
            fields.append(field)
        
        return pa.schema(fields)
    
    def add_inspector(self, inspector: ModelInspector):
        """Add a custom model inspector for other model types."""
        self.inspectors.append(inspector)
    
    def register_field_mapping(self, field_name: str, pyarrow_type: pa.DataType):
        """Register a custom field mapping."""
        self.type_mapper.add_custom_mapping(field_name, pyarrow_type)
    
    def _get_inspector_for_model(self, model_class: Any) -> ModelInspector:
        """Find the appropriate inspector for a given model class."""
        for inspector in self.inspectors:
            if inspector.supports_model(model_class):
                return inspector
        raise ValueError(f"No inspector found for model type: {model_class}")
    
    def _build_schema_from_fields(self, field_info: dict[str, dict[str, Any]]) -> pa.Schema:
        """Build PyArrow schema from extracted field information."""
        fields = []
        
        for field_name, info in field_info.items():
            field_type = info['type']
            nullable = info['nullable']
            
            pyarrow_type = self.type_mapper.python_type_to_pyarrow(
                field_type, field_name
            )
            
            field = pa.field(field_name, pyarrow_type, nullable=nullable)
            fields.append(field)
        
        return pa.schema(fields)
