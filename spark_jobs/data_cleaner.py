import pandas as pd
try:
    # Read and clean data
    df = pd.read_csv("data/raw/sample_npa.csv", skiprows=2)
    
    # Clean column names
    df.columns = [
        'Year', 'Bank', 
        'Gross_NPA_Opening', 'Gross_NPA_Addition',
        'Gross_NPA_Reduction', 'Gross_NPA_WriteOff', 'Gross_NPA_Closing',
        'Net_NPA_Opening', 'Net_NPA_Closing',
        'Normalized_Likelihood', 'Normalized_Impact'
    ]
    
    # Remove summary rows and convert Year to int
    df = df[df['Year'].notna()].copy()
    df['Year'] = df['Year'].astype(int)
    
    # Convert numeric columns
    num_cols = [
        'Gross_NPA_Opening', 'Gross_NPA_Addition',
        'Gross_NPA_Reduction', 'Gross_NPA_WriteOff', 'Gross_NPA_Closing',
        'Net_NPA_Opening', 'Net_NPA_Closing'
    ]
    
    for col in num_cols:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(',', ''), 
            errors='coerce'
        )
    
    # Data validation
    df['Calculated_Closing'] = (
        df['Gross_NPA_Opening'] + 
        df['Gross_NPA_Addition'] - 
        df['Gross_NPA_Reduction'] - 
        df['Gross_NPA_WriteOff']
    )
    
    df['Data_Consistency_Issue'] = (
        abs(df['Gross_NPA_Closing'] - df['Calculated_Closing']) > 1.0
    )
    
    # Risk categorization
    df['Risk_Category'] = pd.cut(
        df['Normalized_Likelihood'] * df['Normalized_Impact'],
        bins=[0, 2, 4, 6, 10, 25],
        labels=['Low', 'Moderate', 'High', 'Very High', 'Extreme']
    )
    
    # Fill NA values - handle categorical column separately
    numeric_cols = df.select_dtypes(include=['number']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # For categorical columns, you might want to fill with a specific category
    if 'Risk_Category' in df.columns:
        df['Risk_Category'] = df['Risk_Category'].cat.add_categories('Unknown').fillna('Unknown')

    df.to_csv('data/processed/output.csv', index=False) 
except:
    print('Error While Cleaning and Refining')