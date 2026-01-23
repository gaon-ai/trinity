"""
Lessee dimension table transformation.

Source tables:
- report_lessee -> dim_lessee (with contract aggregations from report_contract)
"""
import pandas as pd

__all__ = ['create_dim_lessee']


def create_dim_lessee(
    report_lessee: pd.DataFrame,
    report_contract: pd.DataFrame
) -> pd.DataFrame:
    """
    Create dim_lessee by joining lessee data with contract aggregations.

    This dimension table combines lessee master data with summary statistics
    from their contracts (count, first/last contract dates).

    Args:
        report_lessee: Bronze report_lessee DataFrame
        report_contract: Bronze report_contract DataFrame

    Returns:
        Transformed dim_lessee DataFrame

    Output Columns:
        lessee_number: Unique lessee identifier (PK)
        customer_number: Associated customer number
        lessee_name: Full name
        lessee_address_1/2: Address lines
        lessee_city/state_code/zip_code: Location
        lessee_telephone/cellular: Contact numbers
        business_type_description: Type of business
        lessee_birth_date: Date of birth (if individual)
        lessee_language: Preferred language
        email_address: Email contact
        contract_count: Total number of distinct contracts
        first_contract_date: Earliest contract start date
        last_contract_date: Most recent contract start date
        last_updated: Record last update timestamp
    """
    # Aggregate contract info by lessee
    contract_info = report_contract.groupby('lessee_number').agg(
        contract_count=('contract_number', 'nunique'),
        first_contract_date=('contract_start_date', 'min'),
        last_contract_date=('contract_start_date', 'max')
    ).reset_index()

    # Join lessee with contract aggregations
    result = report_lessee.merge(
        contract_info,
        on='lessee_number',
        how='left'
    )

    # Build output DataFrame with renamed columns
    return pd.DataFrame({
        'lessee_number': result['lessee_number'],
        'customer_number': result['customer_no'],
        'lessee_name': result['lessee_name'],
        'lessee_address_1': result['lessee_address_1'],
        'lessee_address_2': result['lessee_address_2'],
        'lessee_city': result['lessee_city'],
        'lessee_state_code': result['lessee_state_code'],
        'lessee_zip_code': result['lessee_zip_code'],
        'lessee_telephone': result['lessee_telephone'],
        'lessee_cellular': result['lessee_cellular'],
        'business_type_description': result['business_type_description'],
        'lessee_birth_date': pd.to_datetime(result['lessee_birth_date'], errors='coerce'),
        'lessee_language': result['lessee_language'],
        'email_address': result['email_address'],
        'contract_count': result['contract_count'].fillna(0).astype(int),
        'first_contract_date': pd.to_datetime(result['first_contract_date'], errors='coerce'),
        'last_contract_date': pd.to_datetime(result['last_contract_date'], errors='coerce'),
        'last_updated': pd.to_datetime(result['lastupdated'], errors='coerce'),
    })
