import pandas as pd
import logging

def transform_data(data):
    try:
        logging.info('\nStarting ICC CWC 2019 ETL process...\n')
        df = pd.DataFrame(data)
         
        logging.info("Column names standardized.")
        df.columns = df.columns.str.upper().str.strip()

        df['MATCH NUMBER'] = df['MATCH NUMBER'].str.replace(r'[^0-9]+', '', regex=True).str.strip()
        df['MATCH NUMBER'] = df['MATCH NUMBER'].astype(int)
        logging.info("MATCH NUMBER cleaned and converted.")

        df = df.rename(columns={'ROUND NUMBER':'MATCH STAGE'})
        df['MATCH STAGE'] = df['MATCH STAGE'].str.replace(r'[^A-Za-z0-9]+','', regex=True).str.strip()
        df['MATCH STAGE'] = df['MATCH STAGE'].replace(['1','2','3','4','5'],'Group Stage')
        df.loc[45,'MATCH STAGE'] = 'Semi Final 1'
        df.loc[46,'MATCH STAGE'] = 'Semi Final 2'
        df.loc[47,'MATCH STAGE'] = 'Final'
        logging.info("MATCH STAGE cleaned.")

        df['DATE'] = df['DATE'].str.replace(r'[^0-9:/ ]+$', '', regex=True).str.strip()
        df['DATE'] = pd.to_datetime(df['DATE'], format='%d/%m/%Y %H:%M')
        df['DATE'] = df['DATE'].dt.date
        logging.info("DATE cleaned and converted to pure date.")
            

        df['LOCATION'] = (
              df['LOCATION']
              .str.replace('@','a')
              .str.replace('+','T')
              .str.replace('3','e')
              .str.strip() )
        df['LOCATION'] = df['LOCATION'].str.replace(r'[^A-Za-z]+','', regex=True).str.title()
        logging.info("LOCATION initial cleaning done.")

        mapping = {
              'Head': 'Headingley',
              'Trent': 'Trent Bridge',
              'Old': 'Old Trafford',
              'Edg': 'Edgbaston',
              'Br': 'Bristol County Ground',
              'Card': 'Sophia Gardens',
              'Emirate': 'The Riverside Ground'
        }

        def replace_location(x):
            for key, val in mapping.items():
                if str(x).startswith(key):
                    return val
            return x

        df['LOCATION'] = df['LOCATION'].apply(replace_location)
        logging.info("Partial stadium names replaced.")

        df['LOCATION'] = df['LOCATION'].str.replace('The', 'The ', 1).str.title()

        df['LOCATION'] = df['LOCATION'].replace(['Dtrafford','Ldtrffrd','Odtraffrd'],'Old Trafford')
        df['LOCATION'] = df['LOCATION'].apply(lambda x: 'The Rose Bowl' if 'Age' in x else x).str.replace('The Geasbwl','The Rose Bowl')
        df['LOCATION'] = df['LOCATION'].replace(['The Val','The Ov','The Ova'],'The Oval').replace(['Rds','The  Riverside Ground'],'The Riverside Ground')
        df['LOCATION'] = df['LOCATION'].replace(['The Countyground','The Cuntyground','The Cuntygrund'],'The County Ground')
        df.loc[47,'LOCATION'] = 'Lords'
        df['LOCATION'] = df['LOCATION'].str.replace('Lords',"Lord's")
        logging.info("LOCATION finalized.")

        df = df.rename(columns={'LOCATION':'STADIUM NAME'})

        stadium_to_city = {
            "Lord's": "London",
            "The Oval": "London",
            "Edgbaston": "Birmingham",
            "Old Trafford": "Manchester",
            "Trent Bridge": "Nottingham",
            "Headingley": "Leeds",
            "Sophia Gardens": "Cardiff",
            "Bristol County Ground": "Bristol",
            "The Riverside Ground": "Durham",
            "The Rose Bowl": "Southampton",
            "The County Ground": "Taunton"
        }

        df['CITY'] = df['STADIUM NAME'].map(stadium_to_city)
        logging.info("CITY column created.")

        df = df[['MATCH NUMBER','HOME TEAM','AWAY TEAM','DATE','STADIUM NAME','CITY','WINNER','PLAYER OF THE MATCH','MATCH STAGE']]
        logging.info("Columns filtered.")

        df['HOME TEAM'] = df['HOME TEAM'].str.replace(r'[^A-Za-z]+','', regex=True).str.title()
        df['AWAY TEAM'] = df['AWAY TEAM'].str.replace(r'[^A-Za-z]+','', regex=True).str.title()
        logging.info("HOME TEAM and AWAY TEAM cleaned.")

        df['HOME TEAM'] = df['HOME TEAM'].apply(lambda x: 'England' if x.startswith('Eng') else x)
        df['HOME TEAM'] = df['HOME TEAM'].apply(lambda x: 'India' if x.startswith('Ind') else x).replace(['Ndia'],'India')
        df['HOME TEAM'] = df['HOME TEAM'].apply(lambda x: 'Australia' if x.startswith('Aus') else x).replace(['Auralia','Utrlia'],'Australia')
        df['HOME TEAM'] = df['HOME TEAM'].apply(lambda x: 'Sri Lanka' if x.startswith('Sr') else x).replace(['Rilank'],'Sri Lanka')
        df['HOME TEAM'] = df['HOME TEAM'].apply(lambda x: 'West Indies' if x.startswith('Wind') else x).replace('Wndie','West Indies')
        df['HOME TEAM'] = df['HOME TEAM'].apply(lambda x: 'New Zealand' if x.startswith('New') or x.startswith('Nw') else x).replace(['Nglnd','Ngand'],'New Zealand')
        df['HOME TEAM'] = df['HOME TEAM'].replace(['Fghnitan','Fghantn','Afghanstn','Afghanisan'],'Afghanistan')
        df['HOME TEAM'] = df['HOME TEAM'].replace(['Pktan','Pkan','Pakn','Pakistn'],'Pakistan')
        df['HOME TEAM'] = df['HOME TEAM'].replace(['Suhafrca','Outhfrca','Outhafric','Southafrica'],'South Africa')
        df['HOME TEAM'] = df['HOME TEAM'].replace(['Bangladsh','Bangdesh','Bangdeh','Bngdesh'],'Bangladesh')
        logging.info("HOME TEAM values corrected.")

        df['AWAY TEAM'] = df['AWAY TEAM'].apply(lambda x: 'England' if x.startswith('Eng') else x)
        df['AWAY TEAM'] = df['AWAY TEAM'].apply(lambda x: 'India' if x.startswith('Ind') else x).replace(['Ndia'],'India')
        df['AWAY TEAM'] = df['AWAY TEAM'].apply(lambda x: 'Australia' if x.startswith('Aus') else x).replace(['Ustria','Ustrla','Uralia','Usra','Ustra'],'Australia')
        df['AWAY TEAM'] = df['AWAY TEAM'].apply(lambda x: 'Sri Lanka' if x.startswith('Sr') else x).replace(['Rianka'],'Sri Lanka')
        df['AWAY TEAM'] = df['AWAY TEAM'].apply(lambda x: 'West Indies' if x.startswith('Wind') else x).replace('Wndie','West Indies')
        df['AWAY TEAM'] = df['AWAY TEAM'].apply(lambda x: 'New Zealand' if x.startswith('New') or x.startswith('Nw') else x).replace(['Nglnd','Ngand'],'New Zealand')
        df['AWAY TEAM'] = df['AWAY TEAM'].replace(['Afghanin','Afghnisan','Afghanstn','Afghanitan','Fghnan'],'Afghanistan')
        df['AWAY TEAM'] = df['AWAY TEAM'].replace(['Pktn','Paktan','Pakn','Pkitan','Paktn','Pkistan'],'Pakistan')
        df['AWAY TEAM'] = df['AWAY TEAM'].replace(['Ouhfrca','Outhfrca','Uthfrica','Southafrca','Suthafric','Suhafrica'],'South Africa')
        df['AWAY TEAM'] = df['AWAY TEAM'].replace(['Bangladsh','Bangdesh','Bngldeh','Bngdesh','Bngadsh'],'Bangladesh')
        logging.info("AWAY TEAM values corrected.")

        df.loc[23,'HOME TEAM'] = 'England'
        df.loc[43,'AWAY TEAM'] = 'India'
        df.loc[40,'HOME TEAM'] = 'England'
        df.loc[37,'HOME TEAM'] = 'England'
        df.loc[37,'AWAY TEAM'] = 'India'
        df.loc[26,'HOME TEAM'] = 'England'
        logging.info("Manual corrections applied.")

        df['WINNER'] = df['WINNER'].str.replace(r'[^A-Za-z]+','', regex=True)
        logging.info("WINNER column cleaned.")

        winners_full = {
            1: ("England", "Ben Stokes"),
            2: ("West Indies", "Oshane Thomas"),
            3: ("New Zealand", "Matt Henry"),
            4: ("Australia", "David Warner"),
            5: ("Bangladesh", "Shakib Al Hasan"),
            6: ("Pakistan", "Mohammad Hafeez"),
            7: ("Sri Lanka", "Nuwan Pradeep"),
            8: ("India", "Rohit Sharma"),
            9: ("New Zealand", "Ross Taylor"),
            10: ("Australia", "Nathan Coulter-Nile"),
            11: ("No Result", ""),
            12: ("England", "Jason Roy"),
            13: ("New Zealand", "James Neesham"),
            14: ("India", "Shikhar Dhawan"),
            15: ("No Result", ""),
            16: ("No Result", ""),
            17: ("Australia", "David Warner"),
            18: ("No Result", ""),
            19: ("England", "Joe Root"),
            20: ("Australia", "Aaron Finch"),
            21: ("South Africa", "Imran Tahir"),
            22: ("India", "Rohit Sharma"),
            23: ("Bangladesh", "Shakib Al Hasan"),
            24: ("England", "Eoin Morgan"),
            25: ("New Zealand", "Kane Williamson"),
            26: ("Australia", "David Warner"),
            27: ("Sri Lanka", "Lasith Malinga"),
            28: ("India", "Jasprit Bumrah"),
            29: ("New Zealand", "Kane Williamson"),
            30: ("Pakistan", "Haris Sohail"),
            31: ("Bangladesh", "Shakib Al Hasan"),
            32: ("Australia", "Aaron Finch"),
            33: ("Pakistan", "Babar Azam"),
            34: ("India", "Virat Kohli"),
            35: ("South Africa", "Dwaine Pretorius"),
            36: ("Pakistan", "Imad Wasim"),
            37: ("Australia", "Alex Carey"),
            38: ("England", "Jonny Bairstow"),
            39: ("Sri Lanka", "Avishka Fernando"),
            40: ("India", "Rohit Sharma"),
            41: ("England", "Jonny Bairstow"),
            42: ("West Indies", "Shai Hope"),
            43: ("Pakistan", "Shaheen Afridi"),
            44: ("India", "Rohit Sharma"),
            45: ("South Africa", "Faf du Plessis"),
            46: ("New Zealand", "Matt Henry"),
            47: ("England", "Chris Woakes"),
            48: ("England", "Ben Stokes"),
        }

        for m, (winner, pom) in winners_full.items():
            df.loc[df["MATCH NUMBER"] == m, "WINNER"] = winner
            if pom:
                df.loc[df["MATCH NUMBER"] == m, "PLAYER OF THE MATCH"] = pom

        df.loc[[10,14,15,17],'PLAYER OF THE MATCH'] = ''
        logging.info("WINNER and PLAYER OF THE MATCH updated.")

        total_runs_dict = {
                        1: 518,
                        2: 213,
                        3: 273,
                        4: 416,
                        5: 557,
                        6: 682,
                        7: 353,
                        8: 457,
                        9: 492,
                        10: 561,
                        11: 0,
                        12: 666,
                        13: 344,
                        14: 668,
                        15: 0,
                        16: 0,
                        17: 573,
                        18: 0,
                        19: 425,
                        20: 581,
                        21: 434,
                        22: 347,
                        23: 654,
                        24: 644,
                        25: 532,
                        26: 714,
                        27: 567,
                        28: 653,
                        29: 411,
                        30: 478,
                        31: 576,
                        32: 400,
                        33: 536,
                        34: 529,
                        35: 640,
                        36: 536,
                        37: 568,
                        38: 491,
                        39: 653,
                        40: 600,
                        41: 491,
                        42: 599,
                        43: 536,
                        44: 529,
                        45: 640,
                        46: 460,
                        47: 449,
                        48: 482
        }


        df["TOTAL RUNS"] = df["MATCH NUMBER"].map(total_runs_dict)
        logging.info("TOTAL RUNS column added.")

        df.columns = df.columns.str.replace(' ','_')
        
        logging.info("ETL process completed.")
       

        return df
    
    except Exception as e:
        logging.error(f'Transformation and Cleaning Failed : {e}')
        return None

    
    finally:
        logging.info('Transformation and Data Cleaning Successful :)')
        logging.info('----------------------------------------------------------------')
       