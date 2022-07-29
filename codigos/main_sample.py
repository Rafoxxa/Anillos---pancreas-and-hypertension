import os
import time

from feature_calculator import FeatureCalculator


## Connect to the database by instanciating FeatureCalculator class
## port 6087 is for chronotype database, port 5087 is fot pancreas
calculator = FeatureCalculator("neo4j://quantitativephysiology.ing.puc.cl:6087", "user", "password", "chronotype")

start_time = time.time()

## Choose participant id
part_id = 16

## Choose one of the two methods below:
## 1) Calculate the VMC along the hole recording period, it takes his time
# vmc_serie = calculator.get_VMC_serie(part_id)
# print(vmc_serie)
# print(f'time of {part_id}: ', time.time() - start_time)

## 2) Calculate the VMC along an explicit range of time defined by me
beg_timestamp = "2019-06-10T19:50:17.117000-0400"
end_timestamp = "2019-06-10T19:53:00.000000-0400"
vmc_serie = calculator.get_VMC_serie_by_date_range(part_id, beg_timestamp, end_timestamp)
print(vmc_serie)
print(f'time of {part_id}: ', time.time() - start_time)

## Save data into pickle file (optional)
save_path = os.path.join(f'participant{part_id:0>2}_vmc.pkl')
vmc_serie.to_pickle(save_path)
print('save to pickle time: ', time.time() - start_time)

calculator.close()

## Check content of created file (optional)
# df = pd.read_pickle(save_path)
# print(df)