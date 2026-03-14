import pandas as pd
import numpy as np

# 1. FUNKSİYA: Adları təmizləmək üçün (Mütləq yuxarıda olmalıdır)
def clean_names(text):
    text = str(text).upper()
    text = text.replace(' İQTİSADİ RAYONU (MANAT)', '')
    text = text.replace(' ŞƏHƏRİ (MANAT)', '')
    text = text.replace(' İQTİSADİ RAYONU', '')
    text = text.replace(' ŞƏHƏRİ', '')
    text = text.replace('(MLN.MANAT)', '')
    text = text.replace('(MANAT)', '')
    return text.strip()

# 2. DATALARI OXUYURUQ
it_hub = pd.read_csv('it-hub-layihsin-qoulan-mussis-siyahs.csv')
mehsul = pd.read_csv('olknin-iqtisadi-rayonlar-uzr-hr-nfrin-dun-mhsul-buraxl-cari-qiym-dt_ba_004.csv')
kredit = pd.read_csv('kredit-qoyulular-dt_bk_009 (1).csv')

# 3. MEHSUL DATASINI HAZIRLAYIRIQ (Unpivot)
mehsul_long = mehsul.melt(id_vars=['Year'], var_name='Region', value_name='Mehsul_Buraxilishi')
mehsul_long['Region_Match'] = mehsul_long['Region'].apply(clean_names)

# 4. IT HUB DATASINI HAZIRLAYIRIQ
it_hub['Rayon_Match'] = it_hub['Rayon'].apply(clean_names)

# 5. KREDİT DATASINI HAZIRLAYIRIQ
# Sənin terminaldakı sütun adlarına görə 'Unnamed: 1' çox gümant ki, region adlarıdır
kredit_long = kredit.copy()
kredit_long.rename(columns={'Unnamed: 1': 'Region_Name', 'Kredit qoyuluşları (mln.manat)': 'Kredit_Hecmi'}, inplace=True)
kredit_long['Region_Match'] = kredit_long['Region_Name'].apply(clean_names)

# 6. AĞILLI MERGE (IT Hub + Mehsul)
economic_regions = mehsul_long['Region_Match'].unique()
def find_matching_region(small_name, big_names_list):
    for big_name in big_names_list:
        if small_name in big_name:
            return big_name
    return None

it_hub['Mapped_Region'] = it_hub['Rayon_Match'].apply(lambda x: find_matching_region(x, economic_regions))
final_df = pd.merge(it_hub, mehsul_long, left_on='Mapped_Region', right_on='Region_Match', how='inner')

# 7. KREDİTİ ƏLAVƏ EDİRİK (İl və Region üzrə)
final_df = pd.merge(final_df, kredit_long[['Year', 'Region_Match', 'Kredit_Hecmi']], 
                    left_on=['Mapped_Region', 'Year'], 
                    right_on=['Region_Match', 'Year'], 
                    how='left')

# 8. YEKUN HESABLAMALAR
final_df['Cemi_IT_Heyet'] = final_df['Pedaqoji'] + final_df['Texniki']
final_df['Mehsul_Buraxilishi'] = pd.to_numeric(final_df['Mehsul_Buraxilishi'], errors='coerce')
final_df['Kredit_Hecmi'] = pd.to_numeric(final_df['Kredit_Hecmi'], errors='coerce')

# 9. YADDA SAXLA
final_df.to_csv('final_analyzed_data_v3.csv', index=False, encoding='utf-8-sig')

print(f"MÖHTƏŞƏM! 3 fayl birləşdirildi. Sətir sayısı: {len(final_df)}")
print("Power BI üçün 'final_analyzed_data_v3.csv' hazırdır.")
