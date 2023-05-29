#----------------------------------------------------------------------------------------------------------------------------
# 
# Script name: Provenance.py
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
# during their processing pipeline. The output is a provenance table in a
# normalized structure. 
# The logical model of the UML class was enhanced with version 2.0.
# Additionally, the export functionality with FHIR provenance resource and W3C PROV.
#
#--------------------------------------------------------------------------------------------------------------------------

import random
import json
import time

from json import JSONEncoder
from collections import OrderedDict
from io import StringIO
from datetime import datetime
from peewee import *

#----------------------------------------------------------------------------------------------------------------------------
#Construction plan for the class 'Provenance
#
# Result of the Provenance class: creation of the tables:
#  Provenance, DataElement, DataStore, Script, DataOwner, DataGovernance, DataSteward
#
# 'DataProvenance':
#    contains Provenance information (normalized form)
# 'DataElement': 
#    contains all variables characterizing a dataelement (variable level metadata)   
# 'DataStore': 
#    contains the file names of the data elements (Data File Level Metadata)
# 'Script':
#    contains all script names with version information (Script Level Metadata)
# 'DataOwner':
#    contains information about responsible data owner of the dataelement
# 'DataGovernance':
#    contains information about valid SOP(s) during processing the dataelement
# 'DataSteward':
#    contains information about responsible person processing the dataelement
# 'Infrastructure':
#    tbd
#
#----------------------------------------------------------------------------------------------------------------------------

class Provenance:

    class DataOwner(Model):
        name = CharField()
        role = CharField()
        type = CharField()
        department = CharField()

        class Meta:
            db_table = 'DataOwner'

    class DataSteward(Model):
        name = CharField()
        role = CharField()
        type = CharField()
        department = CharField()

        class Meta:
            db_table = 'DataSteward'
    
    
    class DataGovernance(Model):
        name = CharField()                  #SOP Nr 1234
        version = CharField()               #V1
        status = CharField()                #approved

        class Meta:
            db_table ='DataGovernance'
    
    class DataStore(Model):
        name = CharField()                # Example: "sap_table"

        class Meta:
            db_table = 'DataStore'
    
    class Script(Model):
        name = CharField()                                                                 # Example "etl_sap.py"
        version = CharField()                                                              # Example "v1.7.1"
        creator = CharField()                                                              # Example "KG"

        class Meta:
            db_table = 'Script'


    class DataElement(Model):
        name = CharField()                                              # Example: "Systolic Blood Pressure"
        description = CharField()                                       # Example: "Extraction of Systolic Blood Pressure from the ICU Tables"
        source = DeferredForeignKey('DataStore', backref='DataElement')
        source_variable = CharField()                                   # Example: "blood_pressure"
        destination = DeferredForeignKey('DataStore', backref='DataElement') 
        destination_variable = CharField()                              # Example: "bp"
        description_of_transformation = CharField()                     # Example: "datetransformation to ISO"
        description_of_qualitycheck = CharField()                       # Example: "rangecheck 10-15"
        script = DeferredForeignKey('Script', backref='DataElement')  
        status_log = CharField()  
        owner = DeferredForeignKey('DataOwner', backref='DataElement')                                             #Example: Department abc
        governance = DeferredForeignKey('DataGovernance', backref='DataElement')
        steward = DeferredForeignKey('DataSteward', backref='DataElement')  

        class Meta:
            db_table = 'DataElement'


    class DataProvenance(Model):     
        dataelement = DeferredForeignKey('DataElement', backref='DataProvenance')
        sourcereference = CharField()                  # Beispiel: 2
        destinationreference = CharField()             # Beispiel: "AB3234A-15"
        quality = CharField()                          # Beispiel: "good"
        timestamp = DateField()                        # Beispiel: "2022-01-02"

        class Meta:
            db_table = 'DataProvenance'


    #---------------------------------------------------------------------------------
    #Constructor: initializes the attributes of the class
    #---------------------------------------------------------------------------------

    
    def __init__(self, db_filename):  # dataelements: enthält wesentliche Metadaten zu den Datenelementen

   
        self.db = SqliteDatabase(db_filename)
        self.db.bind([self.DataOwner, self.DataSteward, self.DataGovernance, self.DataStore, self.Script, self.DataElement, self.DataProvenance])

        self.db.connect()
        self.db.create_tables([self.DataOwner, self.DataSteward, self.DataGovernance, self.Script, self.DataStore, self.DataElement, self.DataProvenance], safe = True)

        # this will ist all defined dataelements (will be defined in add_definition below)
        self.__list_of_dataelements: dict[self.DataElement] = {}

        # make sure the global settings are set to None upon init
        self.__global_script_name: str = None
        self.__global_script_version: str = None
        self.__global_script_creator: str = None
        self.__global_sop_name: str = None
        self.__global_sop_version: str = None
        self.__global_sop_status: str = None
        self.__global_owner_name: str = None
        self.__global_owner_role: str = None
        self.__global_owner_type: str = None
        self.__global_owner_department: str = None
        self.__global_steward_name: str = None
        self.__global_steward_role: str = None
        self.__global_steward_type: str = None
        self.__global_steward_department: str = None        
           
    # Deleting (Calling destructor)
    # Database connection has to be closed when object is destroyed (i.e. to avoid a memory leak)
    
    def __del__(self):
        ''' Closes database connection upon destruction of object '''
        self.db.close()  

    
    def add_script_definition(self, *, script_name: str = None, script_version: str = None, script_creator: str = None) -> None:
        ''' Adds a general script definition (can be overridden by the specific datalelement definition)
        
            Parameters
            ----------
                script_name : str
                    optional: name of the script (usually the filename or e.g. git link)
                script_version : str
                    optional: version identifier of the script
                script_creator : str
                    optional: name of the script creator

            Returns
            -------
            None

        '''

        self.__global_script_name: str = script_name
        self.__global_script_version: str = script_version
        self.__global_script_creator: str = script_creator

        return

    
    def add_governance_definition(self, *, sop_name: str = None, sop_version: str = None, sop_status: str = None) -> None:
        ''' Adds a general governance definition (can be overridden by the specific datalelement definition)
        
            Parameters
            ----------
                sop_name : str
                    optional: name of the SOP (usually the filename or the ID)
                sop_version : str
                    optional: version identifier of the SOP
                sop_status : str
                    optional: validity status of the SOP (normally it should be 'approved')

            Returns
            -------
            None

        '''      

        self.__global_sop_name: str = sop_name
        self.__global_sop_version: str = sop_version
        self.__global_sop_status: str = sop_status

        return

    
    def add_owner_definition(self, *, owner_name: str = None, owner_role: str = None, owner_type: str = None, owner_department: str = None) -> None:
        ''' Adds an ownership definition (can be overridden by the specific datalelement definition)
        
            Parameters
            ----------
                owner_name : str
                    optional: name of the data owner
                owner_role : str
                    optional: what was the owners role (structural or functional)
                owner_type : str
                    optional: how the owner participated (structural or functional)
                owner_department : str
                    optional: name of the owning department

            Returns
            -------
            None

        '''      

        self.__global_owner_name: str = owner_name
        self.__global_owner_role: str = owner_role
        self.__global_owner_type: str = owner_type
        self.__global_owner_department: str = owner_department

        return

    
    def add_steward_definition(self, *, steward_name: str = None, steward_role: str = None, steward_type: str = None, steward_department: str = None) -> None:
        ''' Adds an ownership definition (can be overridden by the specific datalelement definition)
        
            Parameters
            ----------
                steward_name : str
                    optional: name of the data steward
                steward_role : str
                    optional: what was the steward role (structural or functional)
                steward_type : str
                    optional: how the steward participated (structural or functional)
                steward_department : str
                    optional: name of the stewarding department

            Returns
            -------
            None

        '''      

        self.__global_steward_name: str = steward_name
        self.__global_steward_role: str = steward_role
        self.__global_steward_type: str = steward_type
        self.__global_steward_department: str = steward_department

        return


    
    def add_definition(self, *, id, name, description=None, source, source_variable=None, 
                            destination, destination_variable=None, 
                            description_of_transformation=None, description_of_qualitycheck=None,
                            script_name=None, script_version=None, script_creator=None,
                            status_log=None,
                            owner_name=None, owner_role=None, owner_type=None, owner_department=None,
                            steward_name=None,  steward_role=None, steward_type=None, steward_department=None,
                            sop_name=None, sop_version=None, sop_status=None, 
                            ):
        ''' Adds a dataelelement definition to object and creates corrensponding database entries
        
            Parameters
            ----------
                id : str
                    identifier, used to identfy this dataelement definition in make_provenance
                name : str
                    name of this data element
                description: str
                    optional: longer name of the data element
                source : str
                    optional : name of the source data store
                source_variable : str
                    optional: source variable name of this dataelement 
                destination : str
                    optional: name of the destination data store
                destination_variable : str
                    optional: destination variable name of this dataelement
                description_of_transformation : str
                    optional: specification of transformation of this dataelement
                description_of_qualitycheck : str
                    optional: specification of quality check for this dataelement
                script_name : str
                    optional: name of script used to transfer/modify this dataelement
                script_version : str
                    optional: version of script name
                script_creator : str
                    optional: person, who is responsible for this script
                status_log : str
                    optional: information about outcome of the script as stored in the log-file
                owner_name : str
                    optional: name of person and department owning this dataelement
                steward_name : str
                    optional: name of data steward responsible for data management acitivties
                sop_name : str
                    optional: name of valid SOP for proccessing the data element
                sop_version : str
                    optional: version of SOP name
                sop_status : str
                    optional:  status of SOP (draft/approved)

             Returns
             -------
             None      
                    
        '''

        # the user might have globally defined some settings via the self.__global_* variables, make sure we can override them 
        combined_owner_name: str = owner_name if owner_name else self.__global_owner_name
        combined_owner_role: str = owner_role if owner_role else self.__global_owner_role
        combined_owner_type: str = owner_type if owner_type else self.__global_owner_type
        combined_owner_department: str = owner_department if owner_department else self.__global_owner_department
        combined_steward_name: str = steward_name if steward_name else self.__global_steward_name
        combined_steward_role: str = steward_role if steward_role else self.__global_steward_role
        combined_steward_type: str = steward_type if steward_type else self.__global_steward_type
        combined_steward_department: str = steward_department if steward_department else self.__global_steward_department
        combined_sop_name: str = sop_name if sop_name else self.__global_sop_name
        combined_sop_version: str = sop_version if sop_version else self.__global_sop_version
        combined_sop_status: str = sop_status if sop_status else self.__global_sop_status
        combined_script_name: str = script_name if script_name else self.__global_script_name
        combined_script_version: str = script_version if script_version else self.__global_script_version
        combined_script_creator: str = script_creator if script_creator else self.__global_script_creator
    

        # it's ok, if some attributes are undefined, we just don't create the corresponding models
        if combined_owner_name or combined_owner_role or combined_owner_type or combined_owner_department:
            owner, created = self.DataOwner.get_or_create(name=combined_owner_name, role=combined_owner_role, type=combined_owner_type, department=combined_owner_department)
        else:
            owner=None
        
        if combined_steward_name or combined_steward_role or combined_steward_type or combined_steward_department:
            steward, created = self.DataSteward.get_or_create(name=combined_steward_name, role=combined_steward_role, type=combined_steward_type, department=combined_steward_department)
        else:
            steward=None
        
        if combined_sop_name or combined_sop_version or combined_sop_status:
            governance, created =self.DataGovernance.get_or_create(name=combined_sop_name, version=combined_sop_version, status=combined_sop_status)
        else:
            governance=None

        if combined_script_name or combined_script_version or combined_script_creator:
            script, created = self.Script.get_or_create(name=combined_script_name, version=combined_script_version, creator=combined_script_creator) 
        else:
            script=None

        source, created = self.DataStore.get_or_create(name=source)      
        destination, created = self.DataStore.get_or_create(name=destination)      
        dataelement, created = self.DataElement.get_or_create(name=name,
                                                    description=description,
                                                    source=source,
                                                    source_variable=source_variable,
                                                    destination=destination,
                                                    destination_variable=destination_variable,
                                                    description_of_transformation = description_of_transformation,
                                                    description_of_qualitycheck = description_of_qualitycheck,
                                                    script = script,
                                                    status_log=status_log,
                                                    owner = owner,
                                                    governance = governance,
                                                    steward = steward
                                                    )

        self.__list_of_dataelements[id] = dataelement # dieses Object in interner Liste (cache) speichern

    
    def make_provenance(self, *, name, sourcereference, destinationreference, quality, timestamp):
        ''' Write provenance information in the corresponding provenance table
            
            Parameters
            ----------
            name : str
                contains unique reference to the dataelement
            sourcereference : str
                contains unique reference to the source table from which the dataelement steems from
            destinationreference : str
                contains unique reference to the destination table in which the dataelement is stored
            quality : str
                contains information about the quality status of a dataelement
            timestamp : str
                contains timestamp of created provenance record

            Returns
            -------
            None        
        '''

        provinf, created = self.DataProvenance.get_or_create(dataelement=self.__list_of_dataelements[name],
                                            sourcereference=sourcereference,
                                            destinationreference=destinationreference,
                                            quality=quality,
                                            timestamp=timestamp)

        return


    
    def _format_provenance_logtext(self, *, source_name, source_variable, source_reference, script_name, script_version, destination_name, destination_variable, destination_reference, steward_name, steward_role):
        """ Formats provenance information as a text to be copied into a logfile or to be read"""
        return (f"{source_name} {source_variable} {source_reference} "+ 
                                f"was converted by {script_name} {script_version} "+
                                f"to {destination_name} {destination_variable} {destination_reference} "+
                                f"by {steward_name} {steward_role}\n")

    
    def _format_provenance_mermaid_flow(self, *, source_name, source_variable, source_reference, script_name, script_version, destination_name, destination_variable, destination_reference, steward_name, steward_role):
        """ Formats provenance information as a mermaid diagram definition (has to be parsed by mermaid to result in image)"""
        return (f"    {source_name}.{source_variable}.{source_reference}"+ 
                                f"--> |{script_name}. {script_version}  {steward_name}.{steward_role}| "+
                                f"{destination_name}.{destination_variable}.{destination_reference}\n")
                               
    
    def _format_provenance_mermaid_w3cprov(self, *, source_name, source_variable, source_reference, script_name, script_version, destination_name, destination_variable, destination_reference, steward_name, steward_role):
        """ Formats provenance information as a mermaid diagram definition (has to be parsed by mermaid to result in image)"""
        return (f"    A['{script_name}''{script_version}'] -->|used| B(['{source_name}''{source_variable}''{source_reference}'])\n"+
                f"    C(['{destination_name}''{destination_variable}''{destination_reference}']) -->|was generated by| A['{script_name}''{script_version}']\n"+
                f"    A['{script_name}''{script_version}'] -.->|was associated with| D[/'{steward_name}'{steward_role}\] \n"+
                f"    C(['{destination_name}''{destination_variable}''{destination_reference}'])  -->|was derived from| B(['{source_name}''{source_variable}''{source_reference}'])\n"
                )

    #def _format_provenance_fhir(self, *, source_name, source_variable, source_reference, script_name, script_version, destination_name, destination_variable, destination_reference, sop_name, timestamp, steward_name, steward_role, description_of_transformation, owner_type, owner_role):
    def _format_provenance_fhir(self, *, source_name, source_variable, source_reference, script_name, script_version, destination_name, destination_variable, destination_reference, sop_name, timestamp, steward_name, steward_role, description_of_transformation, owner_type, owner_role):

        """ Formats provenance information as a fhir resource"""
       
        fhird=OrderedDict()
        fhird['resourceType'] = 'Provenance'
        fhird['occuredDateTime'] = 'timestamp'
        fhird['recorded'] = 'timestamp'
        fhird['policy'] = sop_name

        fhird['location'] = {'reference': steward_name}

        fhird['authorization'] = {'coding':
                       [{'system': 'http://terminology.hl7.org/CodeSystem/v3-ActReason',
                         'code': 'TRANSRCH',
                         'display': 'My own code'}]}

        fhird['activity'] = {'coding':
                       [{'system': 'http://terminology.hl7.org/CodeSystem/iso-21089-lifecycle',
                         'code': description_of_transformation,
                         'display': 'Transform'}]}

        fhird['basedOn'] = 'ServiceRequest'

        fhird['target'] = [{'reference': 'Observation/omh-fhir-example'}]

        fhird['entity'] = {'role': 'derivation'}
        fhird['entity'] = {'what':
                              {'identifier':
                                  [{'system': 'urn:ietf:rfc:3986',
                                  'value': '243c773b-8936-407e-9c23-270d0ea49cc4',
                                  'display': ''}]
                              }}

        fhird['agent'] = {'type':
                            {'coding':
                              [{'system': 'http://terminology.hl7.org/CodeSystem/provenance-participant-type',
                                'code': owner_type,
                                'display': 'owner_name'}]
                            }}
        fhird['agent'] = {'role':
                            {'coding':
                              [{'system': 'http://terminology.hl7.org/CodeSystem/provenance-participant-type',
                                'code': owner_role,
                                'display': 'owner_name'}]
                            }}
        fhird['agent'] =  {'who': 
                            {'identifier': steward_name,
                              'display': 'mysteward name'}
                          }

        io = StringIO()
        json.dump(fhird, io, indent=4)
        
        print(io.getvalue())
        return(io.getvalue())

        #return (f"FHIR does not work yet\n")

    
    def get_provenance(self, *, datastore: str, variable: str, reference: str, format: str = "logtext", _top_level_recursion: bool = True):
        """ output provenance information in various formats

            Parameters
            ----------
            datastore : str
                name of the datastore of the target dataelement
            variable : str
                name of the variable of the target dataelement
            reference : str
                identifying information (e.g. primary key) the target dataelement  # TODO: optional?
            format : str
                optional : formatting of output, possible values are:
                   "logtext" (default) : output as human readable text
                   "mermaid_flow" : output as a mermaid diagram definition (has to be parsed by mermaid to result in image)
                   "mermaid_w3cprov: output as notation in W3C Prov
                   "fhir" : output as one or several fhir resources
            _top_level_recursion : bool
                optional (private): internal variable signifying the depth of recursion

            Returns
            -------
            provenance_text : str
                string with provenance information formatted in different ways as specified               
        
        """
        sourcestore = self.DataStore.alias() # this is so we can join the same table twice
        destinationstore = self.DataStore.alias() # we only need to do this once, but this is for readability

        provenance_datapoint = (self.DataProvenance.select(self.DataProvenance, self.DataElement, destinationstore, sourcestore, self.Script, self.DataSteward, self.DataOwner)
                                                            .join(self.DataElement, on=(self.DataProvenance.dataelement == self.DataElement.id))
                                                            .switch(self.DataElement)
                                                            .join(destinationstore, on=(self.DataElement.destination == destinationstore.id).alias('destination'))   # alias is needed because peewee is confused by joining twice to same table
                                                            .switch(self.DataElement)
                                                            .join(sourcestore, on=(self.DataElement.source == sourcestore.id).alias('source'))       
                                                            .switch(self.DataElement)
                                                            .join(self.Script, on=(self.DataElement.script == self.Script.id))
                                                            .switch(self.DataElement)
                                                            .join(self.DataSteward, on=(self.DataElement.steward == self.DataSteward.id).alias('steward'))                                                          
                                                            .switch(self.DataElement)
                                                            .join(self.DataOwner, on=(self.DataElement.owner == self.DataOwner.id).alias('owner'))
                                                            .where((self.DataProvenance.destinationreference == str(reference)) & 
                                                                   (destinationstore.name == datastore) &
                                                                   (self.DataElement.destination_variable == variable )) # TODO: think if get_or_none would be better here
                               )

        #print("provenance datapoint", provenance_datapoint)
        
        if provenance_datapoint.count()==0:
            return ""

        provenance_text = ""
        if (format == "mermaid_flow" or format == "mermaid_w3cprov") and _top_level_recursion:
            provenance_text = "graph TD\n"

        for dp in provenance_datapoint:
            if format=="logtext":
                provenance_text += self._format_provenance_logtext(source_name=dp.dataelement.source.name, source_variable=dp.dataelement.source_variable, source_reference=dp.sourcereference,
                                                                    script_name=dp.dataelement.script.name, script_version=dp.dataelement.script.version,
                                                                    destination_name=dp.dataelement.destination.name, destination_variable=dp.dataelement.destination_variable, destination_reference=dp.destinationreference,
                                                                    steward_name=dp.dataelement.steward.name, steward_role=dp.dataelement.steward.role
                                                                   )
            elif format == "mermaid_flow":
                provenance_text += self._format_provenance_mermaid_flow(source_name=dp.dataelement.source.name, source_variable=dp.dataelement.source_variable, source_reference=dp.sourcereference,
                                                                    script_name=dp.dataelement.script.name, script_version=dp.dataelement.script.version,
                                                                    destination_name=dp.dataelement.destination.name, destination_variable=dp.dataelement.destination_variable, destination_reference=dp.destinationreference,
                                                                    steward_name=dp.dataelement.steward.name, steward_role=dp.dataelement.steward.role
                                                                    )
            
            elif format == "mermaid_w3cprov":
                provenance_text += self._format_provenance_mermaid_w3cprov(source_name=dp.dataelement.source.name, source_variable=dp.dataelement.source_variable, source_reference=dp.sourcereference,
                                                                    script_name=dp.dataelement.script.name, script_version=dp.dataelement.script.version,
                                                                    destination_name=dp.dataelement.destination.name, destination_variable=dp.dataelement.destination_variable, destination_reference=dp.destinationreference,
                                                                    steward_name=dp.dataelement.steward.name, steward_role=dp.dataelement.steward.role
                                                                    )

            elif format == "fhir":
                provenance_text += self._format_provenance_fhir(source_name=dp.dataelement.source.name, source_variable=dp.dataelement.source_variable, source_reference=dp.sourcereference,
                                                                    script_name=dp.dataelement.script.name, script_version=dp.dataelement.script.version,
                                                                    destination_name=dp.dataelement.destination.name, destination_variable=dp.dataelement.destination_variable, destination_reference=dp.destinationreference,
                                                                    sop_name=dp.dataelement.governance, timestamp=dp.timestamp, 
                                                                    steward_name=dp.dataelement.steward.name, steward_role=dp.dataelement.steward.role,
                                                                    description_of_transformation=dp.dataelement.description_of_transformation,
                                                                    owner_type=dp.dataelement.owner.type, owner_role=dp.dataelement.owner.role
                                                                    )
            
            provenance_text += self.get_provenance(datastore=dp.dataelement.source.name, variable=dp.dataelement.source_variable, reference=dp.sourcereference, format=format, _top_level_recursion= False)

        return provenance_text


if __name__ == '__main__':
    print(None)
