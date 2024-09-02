import mysql.connector
from mysql.connector import Error
import pandas as pd



try:
    connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            port = 3306
)

    if connection.is_connected():
        print("Connection to MySQL server successful")
        cursor = connection.cursor(buffered=True)
        cursor.execute("CREATE DATABASE IF NOT EXISTS hdb")
        print("Database created successfully")
except Error as e:
    print(f"The error '{e}' occurred")

try:
    cursor.execute(f"""USE hdb""")
    print("hdb connected successfully")
except Error as e:
    print(f"The error '{e}' occurred")

try:
    cursor.execute(f"DROP TABLE IF EXISTS {"hdata"}")
    print("hdata dropped successfully")
except Error as e:
    print(f"The error '{e}' occurred")

try:
    cursor.execute(f"CREATE TABLE IF NOT EXISTS hdata (id TEXT, name TEXT, postal_code TEXT, commune TEXT, numero TEXT, voie TEXT, lieu_dit TEXT, code_insee TEXT, siret TEXT, activite TEXT, contact_url TEXT, site_internet TEXT, longitude TEXT, latitude TEXT, transport_station_presence TEXT, stationnement_presence TEXT, stationnement_pmr TEXT, stationnement_ext_presence TEXT, stationnement_ext_pmr TEXT, cheminement_ext_presence TEXT, cheminement_ext_terrain_stable TEXT, cheminement_ext_plain_pied TEXT, cheminement_ext_ascenseur TEXT, cheminement_ext_nombre_marches TEXT, cheminement_ext_reperage_marches TEXT, cheminement_ext_sens_marches TEXT, cheminement_ext_main_courante TEXT, cheminement_ext_rampe TEXT, cheminement_ext_pente_presence TEXT, cheminement_ext_pente_degre_difficulte TEXT, cheminement_ext_pente_longueur TEXT, cheminement_ext_devers TEXT, cheminement_ext_bande_guidage TEXT, cheminement_ext_retrecissement TEXT, entree_reperage TEXT, entree_vitree TEXT, entree_vitree_vitrophanie TEXT, entree_plain_pied TEXT, entree_ascenseur TEXT, entree_marches TEXT, entree_marches_reperage TEXT, entree_marches_main_courante TEXT, entree_marches_rampe TEXT, entree_marches_sens TEXT, entree_dispositif_appel TEXT, entree_dispositif_appel_type TEXT, entree_balise_sonore TEXT, entree_aide_humaine TEXT, entree_largeur_mini TEXT, entree_pmr TEXT, entree_porte_presence TEXT, entree_porte_manoeuvre TEXT, entree_porte_type TEXT, accueil_visibilite TEXT, accueil_personnels TEXT, accueil_audiodescription_presence TEXT, accueil_audiodescription TEXT, accueil_equipements_malentendants_presence TEXT, accueil_equipements_malentendants TEXT, accueil_cheminement_plain_pied TEXT, accueil_cheminement_ascenseur TEXT, accueil_cheminement_nombre_marches TEXT, accueil_cheminement_reperage_marches TEXT, accueil_cheminement_main_courante TEXT, accueil_cheminement_rampe TEXT, accueil_cheminement_sens_marches TEXT, accueil_chambre_nombre_accessibles TEXT, accueil_chambre_douche_plain_pied TEXT, accueil_chambre_douche_siege TEXT, accueil_chambre_douche_barre_appui TEXT, accueil_chambre_sanitaires_barre_appui TEXT, accueil_chambre_sanitaires_espace_usage TEXT, accueil_chambre_numero_visible TEXT, accueil_chambre_equipement_alerte TEXT, accueil_chambre_accompagnement TEXT, accueil_retrecissement TEXT, sanitaires_presence TEXT, sanitaires_adaptes TEXT, labels TEXT, labels_familles_handicap TEXT, registre_url TEXT, conformite TEXT, web_url TEXT)")
    print("Table created successfully")
except Error as e:
    print(f"The error '{e}' occurred")
try:
    df = pd.read_csv(r"C:\py_work\api_gouv\api_sqlserver_host\data.csv")
    df = df.fillna('')
    for _, row in df.iterrows():
        query = """
            INSERT INTO hdata 
            (id, name, postal_code, commune, numero, voie, lieu_dit, code_insee, siret, activite, contact_url, site_internet, longitude, latitude, transport_station_presence, stationnement_presence, stationnement_pmr, stationnement_ext_presence, stationnement_ext_pmr, cheminement_ext_presence, cheminement_ext_terrain_stable, cheminement_ext_plain_pied, cheminement_ext_ascenseur, cheminement_ext_nombre_marches, cheminement_ext_reperage_marches, cheminement_ext_sens_marches, cheminement_ext_main_courante, cheminement_ext_rampe, cheminement_ext_pente_presence, cheminement_ext_pente_degre_difficulte, cheminement_ext_pente_longueur, cheminement_ext_devers, cheminement_ext_bande_guidage, cheminement_ext_retrecissement, entree_reperage, entree_vitree, entree_vitree_vitrophanie, entree_plain_pied, entree_ascenseur, entree_marches, entree_marches_reperage, entree_marches_main_courante, entree_marches_rampe, entree_marches_sens, entree_dispositif_appel, entree_dispositif_appel_type, entree_balise_sonore, entree_aide_humaine, entree_largeur_mini, entree_pmr, entree_porte_presence, entree_porte_manoeuvre, entree_porte_type, accueil_visibilite, accueil_personnels, accueil_audiodescription_presence, accueil_audiodescription, accueil_equipements_malentendants_presence, accueil_equipements_malentendants, accueil_cheminement_plain_pied, accueil_cheminement_ascenseur, accueil_cheminement_nombre_marches, accueil_cheminement_reperage_marches, accueil_cheminement_main_courante, accueil_cheminement_rampe, accueil_cheminement_sens_marches, accueil_chambre_nombre_accessibles, accueil_chambre_douche_plain_pied, accueil_chambre_douche_siege, accueil_chambre_douche_barre_appui, accueil_chambre_sanitaires_barre_appui, accueil_chambre_sanitaires_espace_usage, accueil_chambre_numero_visible, accueil_chambre_equipement_alerte, accueil_chambre_accompagnement, accueil_retrecissement, sanitaires_presence, sanitaires_adaptes, labels, labels_familles_handicap, registre_url, conformite, web_url) 
            VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query,(row['id'], row['name'], row['postal_code'], row['commune'], row['numero'], row['voie'], row['lieu_dit'], 
    row['code_insee'], row['siret'], row['activite'], row['contact_url'], row['site_internet'], row['longitude'], 
    row['latitude'], row['transport_station_presence'], row['stationnement_presence'], row['stationnement_pmr'], 
    row['stationnement_ext_presence'], row['stationnement_ext_pmr'], row['cheminement_ext_presence'], 
    row['cheminement_ext_terrain_stable'], row['cheminement_ext_plain_pied'], row['cheminement_ext_ascenseur'], 
    row['cheminement_ext_nombre_marches'], row['cheminement_ext_reperage_marches'], row['cheminement_ext_sens_marches'], 
    row['cheminement_ext_main_courante'], row['cheminement_ext_rampe'], row['cheminement_ext_pente_presence'], 
    row['cheminement_ext_pente_degre_difficulte'], row['cheminement_ext_pente_longueur'], row['cheminement_ext_devers'], 
    row['cheminement_ext_bande_guidage'], row['cheminement_ext_retrecissement'], row['entree_reperage'], 
    row['entree_vitree'], row['entree_vitree_vitrophanie'], row['entree_plain_pied'], row['entree_ascenseur'], 
    row['entree_marches'], row['entree_marches_reperage'], row['entree_marches_main_courante'], 
    row['entree_marches_rampe'], row['entree_marches_sens'], row['entree_dispositif_appel'], 
    row['entree_dispositif_appel_type'], row['entree_balise_sonore'], row['entree_aide_humaine'], 
    row['entree_largeur_mini'], row['entree_pmr'], row['entree_porte_presence'], row['entree_porte_manoeuvre'], 
    row['entree_porte_type'], row['accueil_visibilite'], row['accueil_personnels'], 
    row['accueil_audiodescription_presence'], row['accueil_audiodescription'], 
    row['accueil_equipements_malentendants_presence'], row['accueil_equipements_malentendants'], 
    row['accueil_cheminement_plain_pied'], row['accueil_cheminement_ascenseur'], 
    row['accueil_cheminement_nombre_marches'], row['accueil_cheminement_reperage_marches'], 
    row['accueil_cheminement_main_courante'], row['accueil_cheminement_rampe'], 
    row['accueil_cheminement_sens_marches'], row['accueil_chambre_nombre_accessibles'], 
    row['accueil_chambre_douche_plain_pied'], row['accueil_chambre_douche_siege'], 
    row['accueil_chambre_douche_barre_appui'], row['accueil_chambre_sanitaires_barre_appui'], 
    row['accueil_chambre_sanitaires_espace_usage'], row['accueil_chambre_numero_visible'], 
    row['accueil_chambre_equipement_alerte'], row['accueil_chambre_accompagnement'], 
    row['accueil_retrecissement'], row['sanitaires_presence'], row['sanitaires_adaptes'], 
    row['labels'], row['labels_familles_handicap'], row['registre_url'], row['conformite'], row['web_url']
)) 
    connection.commit()
    print("table as been updated")

except Error as e:
    print(f"The error '{e}' occurred")

try:
    
    cursor.execute("SELECT * FROM hdata LIMIT 3")
    result = cursor.fetchall()
    if result is not None:
        for row in result:
           print(row)
    
except Error as e:
    print(f"The error '{e}' occurred")  
try:
        cursor.execute("SELECT COUNT(*) FROM hdata")
        row_count = cursor.fetchone()[0]
        print(f"Table 'hdata' contains {row_count} rows.")
except Error as e:
    print(f"The error '{e}' occurred")  

try :
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()
    print("databases:")
    for d in databases:
        print(d[0])
        
except Error as e:
    print(f"The error '{e}' occurred")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")