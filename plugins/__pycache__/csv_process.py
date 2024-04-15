import pandas as pd
import re

# --------------------------------
# Global functions
# --------------------------------
def generate_template(title, legend_label_list):
    legend_labels_html = ""
    for label in legend_label_list:
        legend_labels_html += f"<li><span style='background:{label['color']};opacity:0.7;'></span>{label['text']}</li>"
    
    template = """
    {% macro html(this, kwargs) %}

    <!doctype html>
    <html lang="en">
    <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    """+ "<title>"+ str(title)+ "</title>"+"""
    <link rel="stylesheet" href="//code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">

    <script src="https://code.jquery.com/jquery-1.12.4.js"></script>
    <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.js"></script>

    <script>
    $( function() {
        $( "#maplegend" ).draggable({
                        start: function (event, ui) {
                            $(this).css({
                                right: "auto",
                                top: "auto",
                                bottom: "auto"
                            });
                        }
                    });
    });

    </script>
    </head>
    <body>


    <div id='maplegend' class='maplegend' 
        style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.8);
        border-radius:6px; padding: 10px; font-size:14px; right: 20px; bottom: 20px;'>
    """+"""
    
    <div class='legend-title'>"""+str(title)+"</div>"+"<div class='legend-scale'>"+"""
    <ul class='legend-labels'>"""+ str(legend_labels_html)+"""</ul>
    </div>
    </div>

    </body>
    </html>

    <style type='text/css'>
    .maplegend .legend-title {
        text-align: left;
        margin-bottom: 5px;
        font-weight: bold;
        font-size: 90%;
        }
    .maplegend .legend-scale ul {
        margin: 0;
        margin-bottom: 5px;
        padding: 0;
        float: left;
        list-style: none;
        }
    .maplegend .legend-scale ul li {
        font-size: 80%;
        list-style: none;
        margin-left: 0;
        line-height: 18px;
        margin-bottom: 2px;
        }
    .maplegend ul.legend-labels li span {
        display: block;
        float: left;
        height: 16px;
        width: 30px;
        margin-right: 5px;
        margin-left: 0;
        border: 1px solid #999;
        }
    .maplegend .legend-source {
        font-size: 80%;
        color: #777;
        clear: both;
        }
    .maplegend a {
        color: #777;
        }
    </style>
    {% endmacro %}"""
    
    return template

def match_col_coordinate_with_election(df_coordinate):
    # Matching the column name for fusioning with Election présidentielle
    df_coordinate.rename(columns={'code':'Code du b.vote'}, inplace=True)
    df_coordinate.rename(columns={'commune_code':'Code du département'}, inplace=True)
    
    df_coordinate['Code du b.vote'] = pd.to_numeric(df_coordinate['Code du b.vote'], errors='coerce')
    df_coordinate['Code du département'] = pd.to_numeric(df_coordinate['Code du département'], errors='coerce')

    df_coordinate['Code du b.vote'] = df_coordinate['Code du b.vote'].astype(float)
    df_coordinate['Code du département'] = df_coordinate['Code du département'].astype(float)
    
    return df_coordinate

# --------------------------------
# 2017, 2022 Election data cleaning
# --------------------------------
def convert_txt_to_csv_2017_2022(txt_file_path, csv_file_path):
    try:
        with open(txt_file_path, 'r', encoding='latin-1') as file:
            first_line = file.readline()
            content = file.read()
            headers = first_line.split(';')
    except Exception as e:
        print(f"Error: Unable to read the text file. {e}")
        return

    content = content.replace(',', '.')
    
    try:
        with open(txt_file_path, 'r', encoding='latin-1') as file:
            last_line = file.readlines()[-1]        
    except Exception as e:
        print(f"Error: Unable to read the text file. {e}")
        return
    
    nb_contents = len(last_line.split(';'))
    nb_headers = len(headers)
    nb_lack_header_cols = nb_contents - nb_headers

    headers += [str(num) for num in range(nb_lack_header_cols)]
    
    try:
        with open(csv_file_path, 'w', newline='') as csvfile:
            csvfile.write(';'.join(headers) + '\n')
            csvfile.write(content)
            
            return headers
    except Exception as e:
        print(f"Error: Unable to write to the CSV file. {e}")
        return
    
def clean_pd_df_election_2017_2022(df_election):
    # Drop Nan data
    df_election = df_election[df_election['Code du département'].notnull()]
    df_election = df_election[df_election['Code de la commune'].notnull()]
    df_election = df_election[df_election['Code du b.vote'].notnull()]

    # Numeric column
    df_election['Code du département'] = pd.to_numeric(df_election['Code du département'], errors='coerce')
    df_election['Code de la commune'] = pd.to_numeric(df_election['Code de la commune'], errors='coerce')
    df_election['Code du b.vote'] = pd.to_numeric(df_election['Code du b.vote'], errors='coerce')
    
    return df_election

def change_col_names_2017_2022(df_election):
    # Define the pattern to match column names ending with '0', '1', '2', etc.
    pattern = r'\d+$'  # Matches names ending with underscore followed by digits

    # Find columns that match the pattern
    matching_columns = [col for col in df_election.columns if re.match(pattern, col)]

    # Define the replacement strings
    replacement_strings=['N°Panneau', 'Sexe', 'Nom', 'Prénom', '% Voix', '% Voix/Ins', '% Voix/Exp']

    # Define the number of columns to iterate over
    num_replacement_cols = len(replacement_strings)
    num_matching_cols = len(matching_columns)

    # Replace the underscores in the column names iterably with the replacement strings
    for i in range(num_matching_cols):
        col_index = (i // num_replacement_cols) + 1
        replacement_index = (i % num_replacement_cols) + 1
        replacement = replacement_strings[replacement_index - 1]
        new_col_name = f"{replacement}_{col_index}"
        df_election.rename(columns={matching_columns[i]: new_col_name}, inplace=True)
    return df_election

def prepare_for_fusion_with_coordinate_2017_2022(df_election):
    # Multiply 1000
    df_election['Code du département'] = (df_election['Code du département'].apply(lambda x: x * 1000)).astype(int)

    # Add Code du département + Code de la commune
    df_election['Code du département'] = (df_election['Code du département'] + df_election['Code de la commune']).astype(int)
    
    return df_election

def process_fusioned_data_2017_2022(df_INNER_JOIN):
    # Matches names ending with underscore followed by digits
    pattern = r"% Voix/Exp(?:_\d+)?"  

    columns_voix_exp_list=[col for col in df_INNER_JOIN.columns if re.match(pattern, col)]
    df_voix_exp=df_INNER_JOIN[columns_voix_exp_list]
    df_voix_exp = df_voix_exp.astype('float64')
    
    nb_candidates=len(columns_voix_exp_list)
    candidates=[]
    
    for num in range(nb_candidates):
        if (num ==0):
            candidates.append({'nom': df_INNER_JOIN["Nom"][0], 'prenom':df_INNER_JOIN["Prénom"][0]})
            continue
        candidates.append({'nom':df_INNER_JOIN[f"Nom_{num}"][0], 'prenom':df_INNER_JOIN[f"Prénom_{num}"][0]})    


    df_INNER_JOIN['NOM_candidat_le_plus_voté']=df_voix_exp.idxmax(axis=1).str.replace('% Voix/Exp','Nom')
    df_INNER_JOIN['PRENOM_candidat_le_plus_voté']=df_voix_exp.idxmax(axis=1).str.replace('% Voix/Exp','Prénom')

    df_INNER_JOIN['% Voix/Exp_le_plus_voté']=df_voix_exp.max(axis=1)  

    for num in range(nb_candidates):
        if (num ==0):
            df_INNER_JOIN.loc[(df_INNER_JOIN['NOM_candidat_le_plus_voté'] =='Nom') | (df_INNER_JOIN['NOM_candidat_le_plus_voté'] =='Nom\n'), 'NOM_candidat_le_plus_voté'] = candidates[0]['nom']
            df_INNER_JOIN.loc[(df_INNER_JOIN['PRENOM_candidat_le_plus_voté'] =='Prénom') | (df_INNER_JOIN['PRENOM_candidat_le_plus_voté'] =='Prénom\n'), 'PRENOM_candidat_le_plus_voté'] = candidates[0]['prenom']
            continue
        df_INNER_JOIN.loc[(df_INNER_JOIN['NOM_candidat_le_plus_voté'] ==f'Nom_{num}'),'NOM_candidat_le_plus_voté'] = candidates[num]['nom']
        df_INNER_JOIN.loc[(df_INNER_JOIN['PRENOM_candidat_le_plus_voté'] ==f'Prénom_{num}'),'PRENOM_candidat_le_plus_voté'] = candidates[num]['prenom']    
    
    return df_INNER_JOIN

# --------------------------------
# 2002, 2007 2012 Election data cleaning
# --------------------------------
def convert_txt_to_csv_2012(txt_file_path, csv_file_path):
    try:
        with open(txt_file_path, 'r', encoding='latin-1') as file:
            # Read all lines into a list
            lines = file.readlines()

            # Filter out lines containing '--'
            filtered_lines = [line for line in lines if '--' not in line]

            # Join the filtered lines back into a single string
            content = ''.join(filtered_lines)            
    except Exception as e:
        print(f"Error: Unable to read the text file. {e}")
        return

    try:
        with open(csv_file_path, 'w', newline='') as csvfile:
            csvfile.write(content)    
            return            
    except Exception as e:
        print(f"Error: Unable to write to the CSV file. {e}")
        return
    
def get_headers_2012(df_election):
    # Delete useless columns
    df_election=df_election.drop(columns=[3,4,5,13])

    # Set the headers according to the information of raw data
    headers=['Tour','Code du département', 'Code de la commune', 'Code du b.vote', 'Inscrits', 'Votants', 'Exprimés', 'No. de dépôt du candidat','Nom du candidat', 'Prénom du candidat','Nombre de voix du candidat']
    df_election.columns = headers
    
    return df_election

def clean_pd_df_election_2012(df_election):    
    # Drop Nan data
    df_election = df_election[df_election['Code du département'].notnull()]
    df_election = df_election[df_election['Code de la commune'].notnull()]
    df_election = df_election[df_election['Code du b.vote'].notnull()]

    # Numeric column
    df_election['Code du département'] = pd.to_numeric(df_election['Code du département'], errors='coerce')
    df_election['Code de la commune'] = pd.to_numeric(df_election['Code de la commune'], errors='coerce')
    df_election['Code du b.vote'] = pd.to_numeric(df_election['Code du b.vote'], errors='coerce')
    
    return df_election

def get_pivot_table_2012(df_election):
    common_columns = ['Tour','Code du département', 'Code de la commune', 'Code du b.vote', 'Inscrits', 'Votants', 'Exprimés']

    df_election['Nom complet du candidat'] = df_election['Nom du candidat'].astype(str) + ' ' + df_election['Prénom du candidat'].astype(str)

    df_PIVOT = df_election.pivot_table(index=common_columns, columns='Nom complet du candidat', values='Nombre de voix du candidat', aggfunc=lambda x: x)
    df_PIVOT.reset_index(inplace=True)
 
    df_PIVOT['Candidat le plus voté'] = df_PIVOT.drop(common_columns, axis=1).idxmax(axis=1)
    df_PIVOT['Vote le plus voté'] = df_PIVOT.apply(lambda row: row[row['Candidat le plus voté']], axis=1)
        
    # Multiply 1000
    df_PIVOT['Code du département'] = (df_PIVOT['Code du département'].apply(lambda x: x * 1000)).astype(int)

    # Add Code du département + Code de la commune
    df_PIVOT['Code du département'] = (df_PIVOT['Code du département'] + df_PIVOT['Code de la commune']).astype(int)

    # Calculate % Voix/Exp_le_plus_voté : 
    df_PIVOT['% Voix/Exp_le_plus_voté'] = round((df_PIVOT['Vote le plus voté']/df_PIVOT['Exprimés'])*100,2)    
    
    return df_PIVOT