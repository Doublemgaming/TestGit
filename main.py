import h5py as h5
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

###Aufgabe 1(a)

#Auslesen der Datei
filename = "data_220208_Grundlagen_der_Digitalisierung.hdf5"
modus = "a"

f =h5.File(filename, modus)

#Deklaration der Variablen für die Kombination meines persönlichen Schlüssels.
grp_Radius = f.get('Radius_1000')
grp_Center = grp_Radius.get('Center_node_J236')
grp_Rescue = grp_Center.get('Rescue_node_J417')

#Datensätze meiner Gruppe als pandas DataFrames auslesen und in Variablen speichern
dataframeRadius = pd.DataFrame(grp_Radius)
dataframeCenter = pd.DataFrame(grp_Center)
dataframeRescue = pd.DataFrame(grp_Rescue)


###Aufgabe 1(b)

def read_hdf5_metadata(file_name, path, att_name):
    """ Liefert den Wert eines bestimmten Attributes eines Objekts in einer HDF5-Datei

    :param file_name: Name des Files, welcher ausgelesen werden soll
    :param path: Pfad des Objektes
    :param att_name: Name des gewünschten Attributes
    :type file_name: str
    :type path: str
    :type att_name: str
    :rtype: pandas DataFrame
    :return: Wert des gewünschten Attributes (pandas DataFrame)
    """
    try:
        modus = "a"
        f = h5.File(file_name, modus)
        a = list(f[path].attrs)
        if a.__contains__(att_name):
            b = list(f[path].attrs[att_name])
            return b
    except ValueError:
        print("This attribute does not exist")

#Auslesen von timestamp
# (alle gleich)
timestamp_reqdemand = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/req_demand', 'timestamp')
timestamp_demand = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/demand', 'timestamp')
timestamp_pressure = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/pressure', 'timestamp')

#Auslesen von simulator
#alle gleich, req_demand hat keine
simulator_demand = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/demand', 'simulator')
simulator_pressure = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/pressure', 'simulator')

#Auslesen von py_version
#alle gleich
py_version_demand = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/demand', 'py_version')
py_version_reqdemand = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/req_demand', 'py_version')
py_version_pressure = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/pressure', 'py_version')

#Auslesen von wntr_version
#alle gleich
wntr_version_demand = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/demand', 'wntr_version')
wntr_version_pressure = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/pressure', 'wntr_version')
wntr_version_reqdemand = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/req_demand', 'wntr_version')

#Auslesen von simulator_version
#alle gleich, req_demand hat keine
simulator_version1 = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/demand', 'simulator_version')
simulator_version2 = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/pressure', 'simulator_version')

#Auslesen von quantity
#verschiedene
quantity_reqdemand = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/req_demand', 'quantity')
quantity_demand = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/demand', 'quantity')
quantity_pressure = read_hdf5_metadata("data_220208_Grundlagen_der_Digitalisierung.hdf5", 'Radius_1000/Center_node_J236/Rescue_node_J417/pressure', 'quantity')

#Auslesen von units von quantities
quantity_pressure_units = f['Radius_1000/Center_node_J236/Rescue_node_J417/pressure'].attrs['units']
quantity_demand_units = f['Radius_1000/Center_node_J236/Rescue_node_J417/demand'].attrs['units']
quantity_reqdemand_units = f['Radius_1000/Center_node_J236/Rescue_node_J417/req_demand'].attrs['units']

### Aufgabe 1(c)

def validate_data(reqdem):
    """prüft die Plausibilität der Daten

    :param reqdem: req_demand
    :type reqdem: pandas DataFrame
    :rtype: console output
    :return: console output, which shows, if the data is plausible or not
    """
    #Read in Simulationdata for Center Node
    center_node = reqdem[0];
    if(np.sum(center_node[4:len(center_node)]) == 0):
        print("Data is valid")
    else:
        print("Data is invalid")


### Aufgabe 2
### (a)

#Auslesen der Werte von demand und req_demand aus meiner Rescue_node J417
panda_dem = pd.DataFrame(f['Radius_1000/Center_node_J236/Rescue_node_J417/demand/block0_values'])
panda_reqdem = pd.DataFrame(f['Radius_1000/Center_node_J236/Rescue_node_J417/req_demand/block0_values'])

def percent_demand(panda_req,panda_demand):
    """liefert ein DataFrame zurück, in welchem die prozentuale Bedarfserfüllung zu jedem Zeitschritt mit enthalten ist

    :param panda_req: DataFrame für die Größe req_demand
    :param panda_demand: DataFrame für die Größe demand
    :type panda_req: pandas DataFrame
    :type panda: pandas DataFrame
    :rtype: pandas DataFrame
    :return: resulting pandas DataFrame
    """
    #Reduce demand to size of request by eliminating the entries for tanks and reservoirs
    panda_demand = panda_demand[0:387]
    #Compute relative Quantities, multiply by 100 to get %
    oldrelative = (panda_req/panda_demand)*100
    #Drop columns with only NaN as members
    relative = oldrelative.dropna(how='all', axis = 1)

    return relative

#save in variable
no_nan = percent_demand(panda_reqdem, panda_dem)

### (b)

#compute mean for the saved variable
mean_no_nan = no_nan.mean(axis=1, skipna = True)
#compute std for the saved variable
std_no_nan = no_nan.std(axis=1, skipna = True)
#add new columns to the DataFrame
no_nan['mean'] = mean_no_nan
no_nan['std'] = std_no_nan

### (c)
def avg_pressure(panda_pressure,junctions):
    """liefert das zeitliche Mittel des Drucks, sowie die Standardabweichung bei fünf Verbraucherknoten (Rescue Nodes)

    :param panda_pressure: Attribut pressure von einem Rescue Node
    :param junctions: Objekt, mit dem man 4 beliebige und meinen Rescue Node auswählten kann
    :type panda_pressure: pandas DataFrame
    :type junctions: pandas DataFrame
    :rtype: pandas DataFrame
    :return: DataFrame mit 5 Rescue Nodes, dem gemittelten Druck den Standartabweichungen der jeweiligen Knoten.
    """


    #Data = pd.DataFrame(panda_pressure['block0_values'])
    #junctions.append(5)
    #Data = Data[junctions]
   # pressure_avg = Data.mean(axis = 0, skipna = True)
  #  pressure_std = Data.std(axis = 0, skipna = True)
    #d_return = pd.DataFrame()
   # d_return['mean'] = pressure_avg
    #d_return['std'] = pressure_std
   # return d_return


### Aufgabe 3
### (a)

f.close()
