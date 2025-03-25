import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin SDK
cred = credentials.Certificate("malaysia-waktu-solat-firebase-adminsdk-oaao1-ba14fefc7a.json")
firebase_admin.initialize_app(cred)

# Firestore client from Firebase Admin SDK
db = firestore.client()

# Fetch data from Firestore with hierarchical structure
def fetch_firestore_data(collection_name):
    data = []
    year_docs = db.collection(collection_name).stream()

    for year_doc in year_docs:
        year = year_doc.id
        year_dict = year_doc.to_dict()
        months = db.collection(collection_name).document(year).collections()

        for month in months:
            month_name = month.id
            zone_docs = month.stream()

            # Extract last_updated timestamp for the month from the year document
            last_updated = year_dict.get('last_updated', {}).get(month_name, None)

            for zone_doc in zone_docs:
                print(f"Fetching data for {year} - {month_name} - {zone_doc.id}")
                zone_data = zone_doc.to_dict()
                
                # Add metadata to the zone data
                zone_data["year"] = year
                zone_data["month"] = month_name
                zone_data["zone"] = zone_doc.id
                # Store last_updated as a separate variable to use later
                zone_data["_last_updated"] = last_updated
                
                data.append(zone_data)

    return data

# Month name to number mapping
month_to_number = {
    'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
    'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
}

# Save data to CSV with specific structure
def save_to_csv(data, output_file):
    # Process data to match the required structure
    processed_data = []
    
    for item in data:
        zone = item.get('zone', '')
        year = item.get('year', '')
        month_name = item.get('month', '')
        month_number = month_to_number.get(month_name, '')
        
        prayer_times = item.get('prayerTime', [])
        
        # Get timestamp field if it exists
        last_updated = item.get('_last_updated', '')
        
        # Process each day's prayer time
        for day_data in prayer_times:
            processed_day = {
                'zone': zone,
                'year': year,
                'month': month_number,
                'tarikh_hijri': day_data.get('hijri', ''),
                'fajar': day_data.get('fajr', ''),
                'syuruk': day_data.get('syuruk', ''),
                'zohor': day_data.get('dhuhr', ''),
                'asar': day_data.get('asr', ''),
                'maghrib': day_data.get('maghrib', ''),
                'isyak': day_data.get('isha', ''),
                'updated_date': last_updated,
                'created_date': last_updated
            }
            processed_data.append(processed_day)
    
    # Create DataFrame with the specified columns
    columns = ['zone', 'year', 'month', 'tarikh_hijri', 'fajar', 'syuruk', 
               'zohor', 'asar', 'maghrib', 'isyak', 'updated_date', 'created_date']
    
    df = pd.DataFrame(processed_data, columns=columns)
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")
    print(f"Total records: {len(df)}")

if __name__ == "__main__":
    collection_name = "waktusolat"  # Replace with your Firestore collection name
    output_file = "Dump-output.csv" 

    data = fetch_firestore_data(collection_name)
    save_to_csv(data, output_file)
