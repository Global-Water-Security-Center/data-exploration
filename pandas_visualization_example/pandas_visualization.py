import pandas as pd
import matplotlib.pyplot as plt

# Load the data
data = pd.read_csv('DP_LIVE_17082023222528203.csv')

# Set the Year column as the index
data.set_index('TIME', inplace=True)
# Plot the data
plt.figure(figsize=(10, 10))

crop_types = set(data['SUBJECT'])
country_ids = set(data['LOCATION'])

for crop_id in crop_types:
    crop_mask = data['SUBJECT']==crop_id
    for country_id in country_ids:
        country_mask = data['LOCATION'] == country_id
        plt.plot(data[crop_mask & country_mask]['Value'], linewidth=0.1, marker='.', label=f'{country_id} {crop_id}')

plt.xlabel('Year')
plt.ylabel('Yield')
plt.title('Crop Yield Over Time')
plt.legend(ncols=4, fontsize='x-small')


# Show the plot
plt.show()
