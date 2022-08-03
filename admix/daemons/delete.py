"""
Daemon data deletion, which can search for single_copy tagged runs and delete them.
Lanqing, Jul 2022
"""

import pymongo
from tqdm import tqdm
from utilix import xent_collection
from admix import utils
from admix import rucio
import numpy as np

class DeleteDaemon(AdmixDaemon):
    def __init__(self):
        self.collection = xent_collection()
        # the dids and rse locations for files you want to delete
        self.tagged_on_rse = np.array([])
        self.tagged_dids = np.array([])
        self.to_delete_on_rse = np.array([])
        self.to_delete_dids = np.array([])

    def build_rse_storage_dict(self):
        """Build a dictionary for rses and the corresponding available storage.
        """
        rse_list = rucio.list_rses()
        rse_dict = {}
        for rse in rse_list:
            rse_name = rse['rse']
            rse_dict[rse_name] = rucio.get_account_usage(rse=rse_name)['bytes_remaining']
        self.rse_dict = rse_dict

    def find_priority(self):
        """Find the dids with corresponding rses of higher priority to delete. 
        The determination of priority is based on the remaining storage. A rse with more 
        available storage will be less priority for deletion.
        """
        # initialization
        unique_tagged_dids = np.unique(self.tagged_dids)
        to_delete_dids = []
        to_delete_on_rse = []
        
        # inquire how much storage in bytes left on each rse
        self.build_rse_storage_dict()
        
        # loop over all unique tagged dids
        for d in tqdm(unique_tagged_dids):
            # select out the rses corresponding to a specific did
            rses = self.tagged_on_rse[self.tagged_dids==d]
            remaining_storage = np.zeros(len(rses))
            
            # check how much storage are left on each rses
            for j,rse in enumerate(rses):
                bytes_remaining = self.rse_dict[rse]
                remaining_storage[j] = int(bytes_remaining)
            # find the rses with maximum storage left
            max_rse = rses[np.argmax(remaining_storage)]
            # and marked the rest 'to_delete'
            for rse in rses:
                if rse != max_rse:
                    to_delete_dids.append(d)
                    to_delete_on_rse.append(rse)

        self.to_delete_on_rse = np.array(to_delete_on_rse)
        self.to_delete_dids = np.array(to_delete_dids)

    def find_single_copy(self, raw_type_only = True, exclude_rses=['UC_DALI_USERDISK']):
        """Search within the RunDB to find the runs with tag single copy.
        parameter
        ---------
        raw_type_only[bool]: Delete the raw type only. Default to True.
        exclude_rses[list]: we will skip the data on RSEs in this list. Default to ['UC_DALI_USERDISK']
        
        return
        ------
        tagged_dids[1darray]: DIDs with single_copy tags. eg. 'xnt_007330:raw_records-rfzvpzj4mf'
        tagged_on_rse[1darray]: RSEs where data are stored. eg. 'NIKHEF2_USERDISK'
        """
        # find tagged _single_copy
        query = {'tags.name': {'$in': ['_single_copy']}}
        n_tagged = self.collection.count(query)
        print('Searching for single copy tags among %s tagged runs...'%(n_tagged))

        # check every tagged run
        for i in tqdm(range(n_tagged)):    
            cursor = self.collection.find(query)[i]
            # check every strax data in the run
            for d in cursor['data']:
                # in case there is absence of did
                if d.get('did'):
                    try: 
                        # Get the dtype from did
                        dtype = d['did'].split(':')[1].split('-')[0] 
                        # We only delete the low level dtypes
                        if raw_type_only:
                            # if rawtype and not on the excluded rses
                            if dtype in utils.RAW_DTYPES and (not(d['location'] in exclude_rses)):
                                np.append(self.tagged_dids, d['did'])
                                np.append(self.tagged_on_rse, d['location'])
                        else:
                            # if not on the excluded rses
                            if d['location'] not in exclude_rses:
                                np.append(self.tagged_dids, d['did'])
                                np.append(self.tagged_on_rse, d['location'])
                    except:
                        # Outliers are junk like {'status': 'transferred', 'type': 'veto_regions', 'checksum': 'shit', 
                        # 'creation_time': datetime.datetime(2020, 7, 29, 16, 50, 58, 365000), 'host': 'rucio-catalogue', 
                        # 'location': 'UC_OSG_USERDISK', 'hash': None}
                        pass
        
        print('Found %s runs to delete'%(len(tagged_dids)))

    def do_task(self, mode='single_copy', raw_type_only=True, exclude_rses=['UC_DALI_USERDISK']):
        if mode == 'single_copy':
            print('Going to delete rawtype data with single_copy tags.')
            # find the single_copy tagged runs
            self.find_single_copy(raw_type_only=raw_type_only, exclude_rses=exclude_rses)
            self.find_priority()
            # delete them one by one
            for i in range(len(self.to_delete_dids)):
                did = self.to_delete_dids[i]
                rse = self.to_delete_on_rse[i]
                to_delete_size_gb = int(int(rucio.get_size_mb(did=did))/1024)
                print('Deleting %s from %s, which takes %s GB.'%(did, rse, to_delete_size_gb))
                rucio.delete_rule(did=did, rse=rse, update_db=True)
                print('Deleted %s from %s. '%(did, rse))

        else:
            raise NotImplementedError