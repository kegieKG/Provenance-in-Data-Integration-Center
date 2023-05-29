#----------------------------------------------------------------------------------------------------------------------------
# 
# Script name: prov_measurements.py
# Script version: 1.0
# Script date: 2023-01-23
# Author: KG
# Reviewer: FS
# Copyright statement / usage restrictions: MIT License (MIT)
#   Copyright (c) 2023 KG, FS
#   Permission is hereby granted, free of charge, to any person obtaining a copy
#   of this software and associated documentation files (the “Software”), 
#   to deal in the Software without restriction, including without limitation
#   the rights to use, copy, modify, merge, publish, distribute, sublicense, 
#   and/or sell copies of the Software, and to permit persons to whom the 
#   Software is furnished to do so, subject to the following conditions:
#   The above copyright notice and this permission notice shall be included in
#   all copies or substantial portions of the Software.
#   The Software is provided “as is”, without warranty of any kind, express or
#   implied, including but not limited to the warranties of merchantability,
#   fitness for a particular purpose and noninfringement. In no event shall the
#   authors or copyright holders be liable for any claim, damages or other 
#   liability, whether in an action of contract, tort or otherwise, arising 
#   from, out of or in connection with the software or the use or other dealings
#   in the Software.
# Contact information: kerstin.gierend@medma.uni-heidelberg.de
#
# Script purpose:
# This code generates traceability and provenance information about individual data elements
# during their processing pipeline. The output is measured execution time.
#
#--------------------------------------------------------------------------------------------------------------------------

import random
import json
import time
import timeit 
from statistics import mean
import os
from provenance import *


#----------------------------------------------------------------------------------------------------
#
# Example of use: create instance/object of the class provenance and allocate to variable 'myprov'
#
# ----------------------------------------------------------------------------------------------------
def testprovenance(filename, iterations):

#------------------------------------------------------------------------------
# Instance creation
#------------------------------------------------------------------------------
    myprov = Provenance(db_filename = filename)
    
    myprov.add_script_definition(script_name='sample_etl.py',
                                    script_version='v1.9',
                                    script_creator='KG')
    myprov.add_owner_definition(owner_name='Prof. Meier',
                                    owner_role='Data Owner',
                                    owner_type='Director',
                                    owner_department='Chirurgie')
    myprov.add_governance_definition(sop_name='SOP z',
                                    sop_version='v5',
                                    sop_status='approved')
    myprov.add_steward_definition(steward_name='Hr. Koch',
                                    steward_role='Data Steward',
                                    steward_type='Programmer',
                                    steward_department='DIC - DBMI')

    myprov.add_definition(id='patid',
                            name='patid',
                            description='Patient Number unique',
                            source='stg_sap_vitalis',
                            source_variable='patid',
                            destination='dwh_vitalis',
                            destination_variable='PID',
                            description_of_transformation='copy',
                            description_of_qualitycheck='range check 1-10000000',
                            status_log='passed  date 12.May2022',
                            owner_name='',
                            owner_role='',
                            owner_type='',
                            owner_department='',
                            steward_name='',
                            steward_role='',
                            steward_type='',
                            steward_department='',
                            sop_name='SOP xy',     # we can override SOP information
                            sop_version='v1',
                            sop_status='approved')
    myprov.add_definition(id='pulse',
                             name='pulse',
                             description='Puls rate',
                             source='stg_sap_vitalis',
                             source_variable='pulse',
                             destination='dwh_vitalis',
                             destination_variable='Pulse',   
                             description_of_transformation='copy',
                             description_of_qualitycheck='range check 40-150',
                             status_log='passed  date 12.May2022')
    myprov.add_definition(id='averagepulse',
                             name='averagepulse',
                             description='average Puls rate',
                             source='dwh_vitalis',
                             source_variable='Pulse',
                             destination='dwh_averages',
                             destination_variable='avg_pulse',   
                             description_of_transformation='averaging',
                             description_of_qualitycheck='range check 40-150',
                             script_name='sample_etl2.py',  # we can override script information
                             script_version='v1.9',
                             script_creator='KG',
                             status_log='passed  date 12.May2022')
    myprov.add_definition(id='syst_blood_pressure',
                             name='syst_blood_pressure',
                             description='Systolic Blood Pressure',
                             source='stg_sap_vitalis',
                             source_variable='SysBP',
                             destination='dwh_vitalis',
                             destination_variable='SBP',
                             description_of_transformation='copy',
                             description_of_qualitycheck='range check 80-160',
                             status_log='passed  date 12.May2022',
                             sop_name='SOP p', # we can override SOP information
                             sop_version='v1.5',
                             sop_status='approved',
                             steward_name='no name given')
    myprov.add_definition(id='diast_blood_pressure',
                             name='diast_blood_pressure',
                             description='Diastolic Blood Pressure',
                             source='stg_sap_vitalis',
                             source_variable='DiaBP',
                             destination='dwh_vitalis',
                             destination_variable='DBP',
                             description_of_transformation='copy',
                             description_of_qualitycheck='range check 50-120',
                             status_log='passed  date 12.May2022',
                             owner_name='Prof. Becker', # we can override owner information
                             owner_role='Data Owner',
                             owner_department='Intensiv',
                             sop_name='SOP xy', # we can override SOP information
                             sop_version='v1',
                             sop_status='approved')
    myprov.add_definition(id='performer',
                             name='performer',
                             description='Performer',
                             source='stg_sap_vitalis',
                             source_variable='Staff member',
                             destination='dwh_vitalis',
                             destination_variable='Performed by',
                             description_of_transformation='copy upper letters',
                             description_of_qualitycheck='name in ABCDE',
                             status_log='passed  date 12.May2022',
                             owner_name='Schwester Anne', # we can override owner information
                             owner_role='Data Owner',
                             owner_type='nurse',
                             owner_department='Chirurgie',
                             sop_name='SOP xy', # we can override SOP information
                             sop_version='v1',
                             sop_status='approved')
    myprov.add_definition(id='date',
                             name='date',
                             description='Date of examination',
                             source='stg_sap_vitalis',
                             source_variable='dat_exam',
                             destination='dwh_vitalis',
                             destination_variable='Date YYYY-MM-DD',
                             description_of_transformation='convert date',
                             description_of_qualitycheck='range check > 01.01.2022',
                             status_log='passed  date 12.May2022',
                             sop_name='SOP xy', # we can override SOP information
                             sop_version='v1',
                             sop_status='approved')
    
    
    #------------------------------------------------------------------------------
    # Write destination table (DataProvenance) to store provenance information
    # for all individual data elements by function/method call
    #------------------------------------------------------------------------------
    for ind in range(1,iterations):
        myprov.make_provenance(name='patid', sourcereference = ind, destinationreference = ind+10000, quality="super", timestamp = datetime.now())
        myprov.make_provenance(name='pulse', sourcereference = ind, destinationreference = ind+10000, quality="low", timestamp = datetime.now())
        myprov.make_provenance(name='averagepulse', sourcereference = ind, destinationreference = ind+10000, quality="low", timestamp = datetime.now())
        myprov.make_provenance(name='syst_blood_pressure', sourcereference = ind, destinationreference = ind+10000, quality="low", timestamp = datetime.now())
        myprov.make_provenance(name='diast_blood_pressure', sourcereference = ind, destinationreference = ind+10000, quality="low", timestamp = datetime.now())
        myprov.make_provenance(name='performer', sourcereference = ind, destinationreference = ind+10000, quality="low", timestamp = datetime.now())
        myprov.make_provenance(name='date', sourcereference = ind, destinationreference = ind+10000, quality="low", timestamp = datetime.now())


#    os.remove(filename)
    return


if __name__ == '__main__':
   
    currentdatetime = str(datetime.now())

    warehousesizes = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 
                      1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 
                      100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 700_000, 800_000, 900_000, 1_000_000)
    num_of_repeats = 5

    print("type,size,dbfilesize,duration_per_element,duration_per_record,min_duration,all_results")
    for warehousesize in warehousesizes:
        results=[]
        filesizes=[]
        for r in range(0,num_of_repeats):
            dbfilename = 'test_prov_7_'+currentdatetime+'_'+str(warehousesize)+'_'+str(r)+'.db'

            result= timeit.timeit('testprovenance(\''+dbfilename+'\','+str(warehousesize)+')', globals=globals(), number=1)
            results.append(result)
            filesizes.append(os.stat(dbfilename).st_size)


        dbfilesize = min(filesizes)
        min_duration = min(results)
        duration_per_record = min_duration / warehousesize
        duration_per_element = duration_per_record / 7

        print("emptydb,"
                +str(warehousesize)+","
                +str(dbfilesize)+","
                +str(duration_per_element)+","
                +str(duration_per_record)+","
                +str(min_duration)+","
                +str(results))

    for warehousesize in warehousesizes:  
        results=[]
        filesizes=[]
        for r in range(0,num_of_repeats):
            dbfilename = 'test_prov_7_'+currentdatetime+'_'+str(warehousesize)+'_'+str(r)+'.db'

            result= timeit.timeit('testprovenance(\''+dbfilename+'\','+str(warehousesize)+')', globals=globals(), number=1)
            results.append(result)
            filesizes.append(os.stat(dbfilename).st_size)

        dbfilesize = min(filesizes)
        min_duration = min(results)
        duration_per_record = min_duration / warehousesize
        duration_per_element = duration_per_record / 7

        print("updatedb,"
                +str(warehousesize)+","  
                +str(dbfilesize)+","
                +str(duration_per_element)+","  
                +str(duration_per_record)+","
                +str(min_duration)+","
                +str(results))



