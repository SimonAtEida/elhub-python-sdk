from datetime import datetime, timedelta
import xmltodict


def parse_metering_poll_response(xml_response: str) -> dict:
    """Parses the xml response from consumption.poll_consumption() into a json object.

    Args:
        xml_response (str): xml response with metering values to be parsed

    Returns:
        dict: Dictionary of the parsed values with Object structure:
        {
            "meterings": metering_list,
            "acknowledgements": acknowledgment_list
        }

    """

    def __acknowledgement_decomposition(acknowledgement_dict: dict) -> dict:
        """Decomposes the acknowledgements from the respsone

        Args:
            acknowledgement_dict (dict): Dictionary of an acknowledgment

        Returns:
            dict: Dictionary of the parsed values with Object structure:
            {
                meterings: [
                    {
                    "metering_point": "string",
                    "timestamp": "string";
                    "identification": "string",
                    "energy_business_process": "string",
                    "start_time": "string",
                    "end_time": "string",
                    "resolution": "string",
                    "unit": "string",
                    "observations": [
                            {
                            "timestamp":  "string",
                            value": "string"
                            }
                        ]
                    }
                ],
                acknowlegments: [
                    {
                    "timestamp": "string",
                    "identification":"string",
                    "energy_business_process":"string",
                    "status_type": "string",
                    "response_reason_type": "string",
                    "original_business_document_reference": "string",
                    }
                ]
            }
        """
        acknowledgment = {}
        acknowledgment["timestamp"] = acknowledgement_dict.get("Header").get("Creation")
        acknowledgment["identification"] = acknowledgement_dict.get("Header").get("Identification")
        acknowledgment["energy_business_process"] = (
            acknowledgement_dict.get("ProcessEnergyContext").get("EnergyBusinessProcess").get("#text")
        )
        acknowledgment["status_type"] = acknowledgement_dict.get("PayloadResponseEvent").get("StatusType").get("#text")
        acknowledgment["response_reason_type"] = (
            acknowledgement_dict.get("PayloadResponseEvent").get("ResponseReasonType").get("#text")
        )
        acknowledgment["original_business_document_reference"] = acknowledgement_dict.get("PayloadResponseEvent").get(
            "OriginalBusinessDocumentReference"
        )

        return acknowledgment

    def __metering_decomposition(metering_dict: dict) -> dict:
        """Decomposes the meterings from the respsone

        Args:
            metering_dict (dict): Dictionary of a metering

        Returns:
            dict: _description_
        """
        metering = {}
        metering["metering_point"] = (
            metering_dict.get("PayloadEnergyTimeSeries")
            .get("MeteringPointUsedDomainLocation")
            .get("Identification")
            .get("#text")
        )
        metering["timestamp"] = metering_dict.get("Header").get("Creation")
        metering["identification"] = metering_dict.get("Header").get("Identification")
        metering["energy_business_process"] = (
            metering_dict.get("ProcessEnergyContext").get("EnergyBusinessProcess").get("#text")
        )
        metering["start_time"] = (
            metering_dict.get("PayloadEnergyTimeSeries").get("ObservationPeriodTimeSeriesPeriod").get("Start")
        )
        metering["end_time"] = (
            metering_dict.get("PayloadEnergyTimeSeries").get("ObservationPeriodTimeSeriesPeriod").get("End")
        )
        metering["resolution"] = (
            metering_dict.get("PayloadEnergyTimeSeries")
            .get("ObservationPeriodTimeSeriesPeriod")
            .get("ResolutionDuration")
        )
        metering["unit"] = (
            metering_dict.get("PayloadEnergyTimeSeries").get("ProductIncludedProductCharacteristics").get("UnitType")
        )

        # Depending on the BRS the observations have a different tag, either Observation or "ProfiledObservation"
        # if no tag matches return none
        if metering_dict.get("PayloadEnergyTimeSeries").get("Observation"):
            observations = metering_dict.get("PayloadEnergyTimeSeries").get("Observation")
            start_time = datetime.strptime(metering.get("start_time"), "%Y-%m-%dT%H:%M:%S%z")
            # Observations can have different quality codes, which alters the structure of the object
            # Possible quality codes: 21 Temporary, 56 Estimated, 58 Withdrawn, 81 Final estimate, 127 Measured
            metering["observations"] = [
                {
                    "timestamp": (start_time + timedelta(hours=int(e.get("@Sequence")))).isoformat(),
                    "value": e.get("Metered"),
                }
                if e.get("Metered")
                else {
                    "timestamp": (start_time + timedelta(hours=int(e.get("@Sequence")))).isoformat(),
                    "value": list(e.values())[1].get("#text"),
                }
                for e in observations
            ]
        elif metering_dict.get("PayloadEnergyTimeSeries").get("ProfiledObservation"):
            start_time = datetime.strptime(metering.get("start_time"), "%Y-%m-%dT%H:%M:%S%z")
            observations = metering_dict.get("PayloadEnergyTimeSeries").get("ProfiledObservation")
            metering["observations"] = {
                "timestamp": start_time.isoformat(),
                "value": observations.get("Metered").get("#text"),
            }
        else:
            return None

        return metering

    json_response = xmltodict.parse(
        xml_response,
        process_namespaces=False,
        namespaces={
            "ns0": "",
            "ns1": "",
            "ns2": "",
            "ns3": "",
            "ns4": "",
            "ns5": "",
            "ns6": "",
            "ns7": "",
        },
    )

    result_data_set = (
        json_response.get(
            "Envelope",
        )
        .get("Body")
        .get("PollForDataResponse")
        .get("ResultDataSet", None)
    )

    if not result_data_set:
        raise Exception("No Data found in the response")

    # If no acknowledgment is found we return an empty array
    acknowledgment = result_data_set.get("Acknowledgement", [])

    # If there is only one acknowledgment as a single dict we wrap it into a list
    if isinstance(acknowledgment, dict):
        acknowledgment = [acknowledgment]

    # If no metering is found we return an empty list
    metering = result_data_set.get("NotifyValidatedDataForBillingEnergy", [])
    # If there is only one metering as a single dict we wrap it into a list
    if isinstance(metering, dict):
        metering = [metering]

    metering_list = [__metering_decomposition(response) for response in metering]

    acknowledgment_list = [__acknowledgement_decomposition(response) for response in acknowledgment]

    return {"meterings": metering_list, "acknowledgements": acknowledgment_list}
