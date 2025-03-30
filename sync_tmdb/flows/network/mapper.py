import pandas as pd

class Mapper:
	@staticmethod
	def network(network: dict) -> pd.DataFrame:
		network_data = [
			{
				"id": network.get("id"),
				"name": network.get("name", None),
				"headquarters": network.get("headquarters", None),
				"homepage": network.get("homepage", None),
				"origin_country": network.get("origin_country", None),
			}
		]
		return pd.DataFrame(network_data)
	
	@staticmethod
	def network_image(network: dict) -> pd.DataFrame:
		networkId = network["id"]
		images = network.get("images", {}).get("logos", [])
		network_image_data = [
			{
				"id": image["id"],
				"network": networkId,
				"file_path": image["file_path"],
				"file_type": image["file_type"],
				"aspect_ratio": image["aspect_ratio"],
				"height": image["height"],
				"width": image["width"],
				"vote_average": image["vote_average"],
				"vote_count": image["vote_count"],
			}
			for image in images
		]

		return pd.DataFrame(network_image_data)
	
	@staticmethod
	def network_alternative_name(network: dict) -> pd.DataFrame:
		networkId = network["id"]
		alternative_names = network.get("alternative_names", {}).get("results", [])
		network_alternative_name_data = [
			{
				"network": networkId,
				"name": alternative_name["name"],
				"type": alternative_name["type"],
			}
			for alternative_name in alternative_names
		]

		return pd.DataFrame(network_alternative_name_data)