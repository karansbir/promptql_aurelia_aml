"""
functions.py

This is an example of how you can use the Python SDK's built-in Function connector to easily write Python code.
When you add a Python Lambda connector to your Hasura project, this file is generated for you!

In this file you'll find code examples that will help you get up to speed with the usage of the Hasura lambda connector.
If you are an old pro and already know what is going on you can get rid of these example functions and start writing your own code.
"""
from hasura_ndc import start
from hasura_ndc.instrumentation import with_active_span # If you aren't planning on adding additional tracing spans, you don't need this!
from opentelemetry.trace import get_tracer # If you aren't planning on adding additional tracing spans, you don't need this either!
from hasura_ndc.function_connector import FunctionConnector
from pydantic import BaseModel, Field # You only need this import if you plan to have complex inputs/outputs, which function similar to how frameworks like FastAPI do
import asyncio # You might not need this import if you aren't doing asynchronous work
from hasura_ndc.errors import UnprocessableContent
from typing import Annotated
import os
import httpx

connector = FunctionConnector()

# This is an example of a simple function that can be added onto the graph
@connector.register_query # This is how you register a query
def hello(name: str) -> str:
    return f"Hello {name}"

# You can use Nullable parameters, but they must default to None
# The FunctionConnector also doesn't care if your functions are sync or async, so use whichever you need!
@connector.register_query
async def nullable_hello(name: str | None = None) -> str:
    return f"Hello {name if name is not None else 'world'}"

# Parameters that are untyped accept any scalar type, arrays, or null and are treated as JSON.
# Untyped responses or responses with indeterminate types are treated as JSON as well!
@connector.register_mutation # This is how you register a mutation
def some_mutation_function(any_type_param):
    return any_type_param

# Similar to frameworks like FastAPI, you can use Pydantic Models for inputs and outputs
class Pet(BaseModel):
    name: str
    
class Person(BaseModel):
    name: str
    pets: list[Pet] | None = None

@connector.register_query
def greet_person(person: Person) -> str:
    greeting = f"Hello {person.name}!"
    if person.pets is not None:
        for pet in person.pets:
            greeting += f" And hello to {pet.name}.."
    else:
        greeting += f" I see you don't have any pets."
    return greeting

class ComplexType(BaseModel):
    lists: list[list] # This turns into a List of List's of any valid JSON!
    person: Person | None = None # This becomes a nullable attribute that accepts a person type from above
    x: int # You can also use integers
    y: float # As well as floats
    z: bool # And booleans

# When the outputs are typed with Pydantic models you can select which attributes you want returned!
@connector.register_query
def complex_function(input: ComplexType) -> ComplexType:
    return input

# This last section shows you how to add Otel tracing to any of your functions!
tracer = get_tracer("ndc-sdk-python.server") # You only need a tracer if you plan to add additional Otel spans

# Utilizing with_active_span allows the programmer to add Otel tracing spans
@connector.register_query
async def with_tracing(name: str) -> str:

    def do_some_more_work(_span, work_response):
        return f"Hello {name}, {work_response}"

    async def the_async_work_to_do():
        # This isn't actually async work, but it could be! Perhaps a network call belongs here, the power is in your hands fellow programmer!
        return "That was a lot of work we did!"

    async def do_some_async_work(_span):
        work_response = await the_async_work_to_do()
        return await with_active_span(
            tracer,
            "Sync Work Span",
            lambda span: do_some_more_work(span, work_response), # Spans can wrap synchronous functions, and they can be nested for fine-grained tracing
            {"attr": "sync work attribute"}
        )

    return await with_active_span(
        tracer,
        "Root Span that does some async work",
        do_some_async_work, # Spans can wrap asynchronous functions
        {"tracing-attr": "Additional attributes can be added to Otel spans by making use of with_active_span like this"}
    )

# This is an example of how to setup queries to be run in parallel
@connector.register_query(parallel_degree=5) # When joining to this function, it will be executed in parallel in batches of 5
async def parallel_query(name: str) -> str:
    await asyncio.sleep(1)
    return f"Hello {name}"

# This is an example of how you can throw an error
# There are different error types including: BadRequest, Forbidden, Conflict, UnprocessableContent, InternalServerError, NotSupported, and BadGateway
@connector.register_query
def error():
    raise UnprocessableContent(message="This is a error", details={"Error": "This is a error!"})

class Foo(BaseModel):
  bar: str = Field(..., description="The bar field") # Add a field description
  baz: Annotated[str, "The baz field"] # A different way to add a field description

# You can use Field or Annotated to add descriptions to the metadata
@connector.register_query
def annotations(foo: Foo | None = Field(..., description="The optional input Foo")) -> Foo | None:
    """Writing a doc-string like this will become the function/procedure description"""
    return None

# OpenSanctions OFAC SDN Models
class OfacSdnEntity(BaseModel):
    id: str = Field(..., description="Unique entity identifier")
    name: str = Field(..., description="Entity name")
    entity_type: str = Field(..., description="Type of entity (Person, Organization, etc.)")
    country: str | None = Field(None, description="Country of origin")
    address: str | None = Field(None, description="Address")
    birth_date: str | None = Field(None, description="Birth date")
    listed_date: str = Field(..., description="Date when entity was listed")
    program: str = Field(..., description="Sanctions program")
    list_type: str = Field(..., description="Type of sanctions list")
    score: float | None = Field(None, description="Matching score")

class SearchOfacSdnResponse(BaseModel):
    success: bool = Field(..., description="Whether the search was successful")
    results: list[OfacSdnEntity] = Field(..., description="Search results")
    total_count: int = Field(..., description="Total number of results")
    error: str | None = Field(None, description="Error message if search failed")

class MatchOfacEntityResponse(BaseModel):
    success: bool = Field(..., description="Whether the match was successful")
    matches: list[OfacSdnEntity] = Field(..., description="Matching entities")
    error: str | None = Field(None, description="Error message if match failed")

# OpenSanctions OFAC SDN Search Function
@connector.register_query
async def search_ofac_sdn(query: str, limit: int | None = 10) -> SearchOfacSdnResponse:
    """Search the OFAC SDN sanctions database for entities matching the query"""
    try:
        api_key = os.getenv("APP_OFAC_OPENSANCTIONS_API_KEY")
        if not api_key:
            return SearchOfacSdnResponse(
                success=False,
                results=[],
                total_count=0,
                error="OpenSanctions API key not configured"
            )

        params = {"q": query}
        if limit:
            params["limit"] = str(limit)

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://api.opensanctions.org/search/us_ofac_sdn",
                params=params,
                headers=headers
            )

            if response.status_code != 200:
                return SearchOfacSdnResponse(
                    success=False,
                    results=[],
                    total_count=0,
                    error=f"HTTP {response.status_code}: {response.text}"
                )

            data = response.json()
            results = []
            
            for item in data.get("results", []):
                # OpenSanctions API returns data in 'properties' nested structure
                props = item.get("properties", {})
                
                # Extract first value from arrays or use direct values
                def get_first_or_value(prop_list):
                    if isinstance(prop_list, list) and prop_list:
                        return prop_list[0]
                    return prop_list if prop_list else ""
                
                entity = OfacSdnEntity(
                    id=item.get("id", ""),
                    name=item.get("caption", get_first_or_value(props.get("name", []))),
                    entity_type=item.get("schema", ""),  # Person, Organization, etc.
                    country=get_first_or_value(props.get("country", [])),
                    address=get_first_or_value(props.get("address", [])),
                    birth_date=get_first_or_value(props.get("birthDate", [])),
                    listed_date=get_first_or_value(props.get("createdAt", [])),
                    program=get_first_or_value(props.get("programId", [])),
                    list_type="SDN",  # This is OFAC SDN list
                    score=item.get("score")  # Score might be added by search relevance
                )
                results.append(entity)

            # Handle total_count which can be an object or integer
            total_count = data.get("total", 0)
            if isinstance(total_count, dict):
                total_count = total_count.get("value", 0)

            return SearchOfacSdnResponse(
                success=True,
                results=results,
                total_count=total_count
            )

    except Exception as e:
        return SearchOfacSdnResponse(
            success=False,
            results=[],
            total_count=0,
            error=str(e)
        )

@connector.register_query
async def match_ofac_entity(name: str, country: str | None = None, entity_type: str | None = None) -> MatchOfacEntityResponse:
    """Match an entity against the OFAC SDN sanctions database"""
    try:
        api_key = os.getenv("APP_OFAC_OPENSANCTIONS_API_KEY")
        if not api_key:
            return MatchOfacEntityResponse(
                success=False,
                matches=[],
                error="OpenSanctions API key not configured"
            )

        # Use search API instead of reconcile since match format is complex
        params = {"q": name}
        if country:
            params["countries"] = country
        
        # Note: We'll use search API for now since reconcile API format is complex

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://api.opensanctions.org/search/us_ofac_sdn",
                params=params,
                headers=headers
            )

            if response.status_code != 200:
                return MatchOfacEntityResponse(
                    success=False,
                    matches=[],
                    error=f"HTTP {response.status_code}: {response.text}"
                )

            data = response.json()
            matches = []
            
            # Using search API format (same as search function)
            for item in data.get("results", []):
                # OpenSanctions API returns data in 'properties' nested structure
                props = item.get("properties", {})
                
                # Extract first value from arrays or use direct values
                def get_first_or_value(prop_list):
                    if isinstance(prop_list, list) and prop_list:
                        return prop_list[0]
                    return prop_list if prop_list else ""
                
                entity = OfacSdnEntity(
                    id=item.get("id", ""),
                    name=item.get("caption", get_first_or_value(props.get("name", []))),
                    entity_type=item.get("schema", ""),  # Person, Organization, etc.
                    country=get_first_or_value(props.get("country", [])),
                    address=get_first_or_value(props.get("address", [])),
                    birth_date=get_first_or_value(props.get("birthDate", [])),
                    listed_date=get_first_or_value(props.get("createdAt", [])),
                    program=get_first_or_value(props.get("programId", [])),
                    list_type="SDN",  # This is OFAC SDN list
                    score=item.get("score")  # Score might be added by search relevance
                )
                matches.append(entity)

            return MatchOfacEntityResponse(
                success=True,
                matches=matches
            )

    except Exception as e:
        return MatchOfacEntityResponse(
            success=False,
            matches=[],
            error=str(e)
        )

# Test function to check API key environment variable
class ApiKeyTestResponse(BaseModel):
    api_key_exists: bool = Field(..., description="Whether the API key environment variable exists")
    api_key_length: int = Field(..., description="Length of the API key (0 if not found)")
    api_key_preview: str = Field(..., description="First 10 characters of API key (masked if longer)")
    env_var_name: str = Field(..., description="Environment variable name being checked")

@connector.register_query
def test_api_key_env() -> ApiKeyTestResponse:
    """Test function to verify OpenSanctions API key is properly configured in environment"""
    env_var_name = "APP_OFAC_OPENSANCTIONS_API_KEY"
    api_key = os.getenv(env_var_name)
    
    if api_key:
        api_key_length = len(api_key)
        # Show first 10 chars for verification, mask the rest
        if api_key_length > 10:
            api_key_preview = api_key[:10] + "..." + ("*" * (api_key_length - 10))
        else:
            api_key_preview = api_key
        
        return ApiKeyTestResponse(
            api_key_exists=True,
            api_key_length=api_key_length,
            api_key_preview=api_key_preview,
            env_var_name=env_var_name
        )
    else:
        return ApiKeyTestResponse(
            api_key_exists=False,
            api_key_length=0,
            api_key_preview="[NOT FOUND]",
            env_var_name=env_var_name
        )

if __name__ == "__main__":
    start(connector)
