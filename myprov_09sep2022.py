#-------------------------------------------------------------------------------
# 
# Script name: myProvenance.py
# Script version: 1.0
# Script date: 2022-09-09
# Author: Kerstin Gierend
# Reviewer: Dr. Fabian Siegel
# Copyright statement / usage restrictions:
# Contact information: kerstin.gierend@medma.uni-heidelberg.de
#
# Script purpose:
# This code generates traceability information about individual data elements
# during their processing pipeline. The output is a provenance table in a
# normalized structure. Further enhancements are planned for version 2.0. 
#
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# libraries
#-------------------------------------------------------------------------------
import numpy as np
import pandas as pd
from datetime import datetime
import os

#-------------------------------------------------------------------------------
# read input file with examplary data elements
#-------------------------------------------------------------------------------
df = pd.read_csv("/Volumes/Kerstin SSD/PhD/Umsetzung/Provenance_Code/inputfile1.txt", sep=";")
df

#-------------------------------------------------------------------------------
# Dies ist der Bauplan der Klasse 'Provenance'.
#  Die Klasse 'Provenance' besitzt Eigenschaften (Attribute), die in _init_ 
#  definiert sind.
#  Die Funktionalität/die Methode des Objektes wird in der Methode 
#  'make_provenance' festgelegt.
#  Von der Klasse 'Provenance' wird die Instanz bzw. das Objekt 'myprov_rec'
#  erzeugt. 
#
# Ergebnis der Klasse Provenance:
#  myProvTab: enthält Provenance-Daten in normalisierter Form
#  myDFLM:    enthält die 'Source' Dateinamen der Datenelemente 
#             (Data File Level Metadata)
#  mySLM:     enthält alle Scriptnamen mit Versionsangabe
#             (Script Level Metadata)
#  myVLM:     enthält alle Variablen inklusive aller Datenelemente, 
#             mit Provenance-Daten (Variable Level Metadata)
#  myVLM_s:   abgeleitet aus myVLM und enthält nur noch eindeutige (unique)
#             Variablen mit deren Provenance-Daten, teil-normalisiert
#-------------------------------------------------------------------------------
class Provenance:

  myProvTab = None
  myDFLM = None
  mySLM = None
  myVLM = None
  
  def __init__(self, myProvTab, myDFLM, mySLM, myVLM, in_source, output, 
               script, variables=[]): 

    #Instanzen der Klasse Provenance, stellvertretend für jedes neue Objekt,
    #welches in der Klasse angelegt wird
    #self.script = script
    self.in_source = in_source
    self.output = output
    self.variables = variables
    self.script = script
    self.myProvTab=myProvTab
    self.myDFLM=myDFLM
    self.mySLM=mySLM
    self.myVLM=myVLM

    self.myDFLM = self.myDFLM.append({'DataFile_id': '',
                                      'DataFile_name_in': in_source,
                                      'DataFile_name_out': output
                                      },
                                       ignore_index = True)
    
    self.mySLM = self.mySLM.append({'ScriptFile_id': '',
                                    'ScriptFile_name': script
                                     },
                                     ignore_index = True)
    
  def make_provenance(self, VLM_id, DE_name, DE_ref, DE_destination_id, 
                      DE_quality_stat, timestamp, DE_provenance=[]):

    self.VLM_id = VLM_id
    self.DE_ref = DE_ref
    self.DE_name = DE_name
    #self.DE_value = DE_value
    self.DE_destination_id = DE_destination_id
    self.DE_quality_stat = DE_quality_stat
    self.DE_provenance = DE_provenance
    self.timestamp = timestamp
    self.myProvTab = self.myProvTab.append({'VLM_id': self.VLM_id,
                                            #'DE_name': self.DE_name,
                                            'DE_ref': self.DE_ref, 
                                            #'DE_value': self.DE_value,
                                            'DE_destination_id': self.DE_destination_id,
                                            'DE_quality_stat': self.DE_quality_stat,
                                            #'DE_provenance': self.DE_provenance,
                                            'timestamp': self.timestamp
                                             },
                                             ignore_index = True)

    self.myVLM = self.myVLM.append({'VLM_id': self.VLM_id,
                                    'Variable_name': self.DE_name,
                                    'Variable_label': 'not available',
                                    'Variable_provenance': self.DE_provenance
                                    },
                                    ignore_index = True)

    self.myVLM_s = myprov_rec.myVLM.drop_duplicates(subset='VLM_id')
    self.myVLM_s = pd.concat([self.myVLM_s, self.myVLM_s["Variable_provenance"].apply(pd.Series)], axis=1)

#-------------------------------------------------------------------------------
# Instanz/Objekt der Klasse Provenance erzeugen und der Variable myprov_rec
# zuordnen
#-------------------------------------------------------------------------------
myprov_rec = Provenance(myProvTab = pd.DataFrame(columns = ["VLM_id", "DE_ref", "DE_destination_id", "DE_quality_stat", "timestamp"]),
                        myDFLM = pd.DataFrame(columns = ["DataFile_id", "DataFile_name_in", "DataFile_name_out"]),
                        mySLM = pd.DataFrame(columns = ["ScriptFile_id", "ScriptFile_name"]),
                        myVLM = pd.DataFrame(columns = ["VLM_id", "Variable_name", "Variable_label", "Variable_provenance"]),           
                        in_source = 'stg_sap.vitalis',
                        output = 'dwh.vitals',   # BEIDE müssen in die variablen-definition
                        script='sample_etl.py, v1.7',
                        variables = {'patid': {'in_source': 'stg_sap_vitalis',
                                               'derivation':'PID',
                                               'description_of_transformation':'copy',
                                               'description_of_qualitycheck':'range check 1-10000000',
                                               'script': 'sample_etl.py, v1.7'
                                              },
                                     'pulse': {'in_source': 'stg_sap_vitalis',
                                               'derivation':'Pulse',   
                                               'description_of_transformation':'copy',
                                               'description_of_qualitycheck':'range check 40-150',
                                               'script': 'sample_etl.py, v1.7'
                                              },
                       'syst_blood_pressure': {'in_source': 'stg_sap_vitalis',
                                               'derivation':'SysBP',
                                               'description_of_transformation': 'copy',
                                               'description_of_qualitycheck':'range check 80-160',
                                               'script': 'sample_etl.py, v1.7'
                                              },
                      'diast_blood_pressure': {'in_source': 'stg_sap_vitalis',
                                               'derivation': 'DiasBP',
                                               'description_of_transformation': 'copy',
                                               'description_of_qualitycheck': 'range check 50-120',
                                               'script': 'sample_etl.py, v1.7'
                                              },
                                 'performer': {'in_source': 'stg_sap_vitalis',
                                               'derivation':'Performer',
                                               'description_of_transformation':'copy upper letters',
                                               'description_of_qualitycheck':'name in ABCDE',
                                               'script': 'sample_etl.py, v1.7'
                                              },
                                      'date': {'in_source': 'stg_sap_vitalis',
                                               'derivation':'Date YYYY-MM-DD',
                                               'description_of_transformation':'convert date',
                                               'description_of_qualitycheck':'range check > 01.01.2022',
                                               'script': 'sample_etl.py, v1.7'
                                              }
                                   }
                      )

#-------------------------------------------------------------------------------
# Zieltabelle (myProvTab) schreiben zum Speichern der Provenance Daten
# über die einzelnen Datenelemente.
# Die Variablen 'DE_destination_id' und 'DE_quality_stat' müssen von ETL Job
# übergeben werden.
# Provenance muss in dieser Version für jede Variable seperat aufgerufen werden.
# In der nächsten Version soll eine Convenience-Funktion erstellt werden, die
# automatisch über die einzelnen Datenelemente interiert.
# Dazu soll diese Funktion direkt in der __init__ erstellt werden.
#-------------------------------------------------------------------------------

for ind in df.index:
  
  myprov_rec.make_provenance(VLM_id='1', DE_name='patid', DE_ref = ind, DE_destination_id="comes from ETL Job",  DE_quality_stat = "comes from ETL Job", DE_provenance = myprov_rec.variables['patid'], timestamp = datetime.now())
  myprov_rec.make_provenance(VLM_id='2', DE_name='pulse', DE_ref = ind, DE_destination_id="comes from ETL Job",  DE_quality_stat = "comes from ETL Job", DE_provenance = myprov_rec.variables['pulse'], timestamp = datetime.now())
  myprov_rec.make_provenance(VLM_id='3', DE_name='syst_blood_pressure', DE_ref = ind, DE_destination_id="comes from ETL Job", DE_quality_stat = "comes from ETL Job", DE_provenance = myprov_rec.variables['syst_blood_pressure'], timestamp = datetime.now())
  myprov_rec.make_provenance(VLM_id='4', DE_name='diast_blood_pressure', DE_ref = ind, DE_destination_id="comes from ETL Job",  DE_quality_stat = "comes from ETL Job", DE_provenance = myprov_rec.variables['diast_blood_pressure'], timestamp = datetime.now())
  myprov_rec.make_provenance(VLM_id='5', DE_name='performer', DE_ref = ind, DE_destination_id="comes from ETL Job",  DE_quality_stat = "comes from ETL Job", DE_provenance = myprov_rec.variables['performer'], timestamp = datetime.now())
  myprov_rec.make_provenance(VLM_id='6', DE_name='date', DE_ref = ind, DE_destination_id="comes from ETL Job",  DE_quality_stat = "comes from ETL Job", DE_provenance = myprov_rec.variables['date'], timestamp = datetime.now())

#-------------------------------------------------------------------------------
# Anschauen/editieren der myProvTab
#-------------------------------------------------------------------------------
myprov_rec.myProvTab

#-------------------------------------------------------------------------------
# Anschauen/editieren der myVLM_s
#-------------------------------------------------------------------------------
myprov_rec.myVLM_s

#-------------------------------------------------------------------------------
# Ausgabe der einzelnen Ergebnistabellen
#-------------------------------------------------------------------------------
print("myProvTab:", myprov_rec.myProvTab)
print("DFLM:", myprov_rec.myDFLM)
print("SLM:", myprov_rec.mySLM)
print("myVLM:", myprov_rec.myVLM)
print("myVLM_s:", myprov_rec.myVLM_s)