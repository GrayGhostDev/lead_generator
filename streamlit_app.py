import streamlit as st
import os
import pandas as pd
from tempfile import TemporaryDirectory
from lead_gen import LeadGenerator, ZoomInfoEnricher
from csv_data_manager import CSVDataManager
from datetime import datetime

st.set_page_config(page_title="People Data Enrichment (ZoomInfo)", layout="wide")
st.title("People Data Enrichment Tool (ZoomInfo)")

st.markdown("""
Upload one or more CSV files with people data. Set enrichment options, then click **Start Enrichment**. 
You will be able to download the enriched and error files after processing.
""")

# Upload CSV files
uploaded_files = st.file_uploader("Upload CSV file(s) for enrichment", type=["csv"], accept_multiple_files=True)

# Enrichment options
col1, col2, col3, col4 = st.columns(4)
batch_size = col1.number_input("Batch Size", min_value=1, max_value=100, value=10)
max_workers = col2.number_input("Max Workers", min_value=1, max_value=16, value=4)
retries = col3.number_input("Retries", min_value=0, max_value=10, value=2)
output_dir = col4.text_input("Output Directory", value="output")

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Start button
enrich_btn = st.button("Start Enrichment", disabled=not uploaded_files)

if enrich_btn and uploaded_files:
    st.info("Starting enrichment. This may take a few minutes depending on file size and API speed.")
    summary_rows = []
    csv_manager = CSVDataManager()
    enricher = ZoomInfoEnricher()
    progress_bar = st.progress(0, text="Processing files...")
    total_files = len(uploaded_files)
    for idx, uploaded_file in enumerate(uploaded_files):
        file_name = uploaded_file.name
        st.write(f"Processing: {file_name}")
        # Save uploaded file to temp location
        with TemporaryDirectory() as tmpdir:
            tmp_path = os.path.join(tmpdir, file_name)
            with open(tmp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            # Process file
            lead_gen = LeadGenerator(enricher=enricher, output_dir=output_dir)
            contacts_df = csv_manager.read_csv(tmp_path)
            total_contacts = len(contacts_df) if not contacts_df.empty else 0
            processed_contacts = []
            errors = []
            contact_list = contacts_df.to_dict('records')
            for i in range(0, total_contacts, batch_size):
                batch = contact_list[i:i + batch_size]
                batch_success = False
                for batch_attempt in range(retries + 1):
                    try:
                        if lead_gen.enricher:
                            enriched_batch = lead_gen.enricher.enrich_contact_batch(batch)
                        else:
                            enriched_batch = batch
                        for orig, enriched in zip(batch, enriched_batch):
                            first = enriched.get('first_name', '') or enriched.get('First Name', '')
                            last = enriched.get('last_name', '') or enriched.get('Last Name', '')
                            name = f"{first} {last}".strip()
                            email = enriched.get('email', '') or enriched.get('Email', '')
                            phone = enriched.get('phone', '') or enriched.get('DirectPhone', '') or enriched.get('Contact Phone', '')
                            if not (name or email or phone):
                                raise ValueError("No name, email, or phone found after enrichment.")
                            processed_contacts.append({
                                'Name': name,
                                'Email': email,
                                'Contact Phone': phone
                            })
                        batch_success = True
                        break
                    except Exception as e:
                        if batch_attempt < retries:
                            continue
                        else:
                            for contact in batch:
                                errors.append({
                                    'contact': contact,
                                    'error': f'Batch enrichment error: {str(e)}'
                                })
                st.write(f"Processed {min(i+batch_size, total_contacts)}/{total_contacts} contacts in {file_name}")
            enriched_df = pd.DataFrame(processed_contacts)
            num_success = len(enriched_df)
            error_rows = []
            for err in errors:
                row = err['contact'].copy()
                row['error'] = err['error']
                error_rows.append(row)
            error_df = pd.DataFrame(error_rows)
            # Save outputs
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base = os.path.splitext(file_name)[0]
            enriched_path = os.path.join(output_dir, f"enriched_contacts_{base}_{timestamp}.csv")
            error_path = os.path.join(output_dir, f"enrichment_errors_{base}_{timestamp}.csv")
            enriched_df.to_csv(enriched_path, index=False)
            if not error_df.empty:
                error_df.to_csv(error_path, index=False)
            else:
                error_path = ''
            summary_rows.append({
                'File': file_name,
                'Processed': num_success,
                'Errors': len(error_df),
                'Output File': enriched_path,
                'Error File': error_path
            })
        progress_bar.progress((idx+1)/total_files, text=f"Processed {idx+1} of {total_files} files")
    # Show summary
    st.success("Enrichment complete!")
    st.markdown("### Summary")
    summary_df = pd.DataFrame(summary_rows)
    st.dataframe(summary_df)
    # Download links
    for row in summary_rows:
        st.markdown(f"**{row['File']}**")
        st.download_button("Download Enriched CSV", data=open(row['Output File'], 'rb').read(), file_name=os.path.basename(row['Output File']))
        if row['Error File']:
            st.download_button("Download Error CSV", data=open(row['Error File'], 'rb').read(), file_name=os.path.basename(row['Error File'])) 