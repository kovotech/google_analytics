from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    RunReportResponse,
    Filter,
    FilterExpression,
    FilterExpressionList
)

class ConstructGA4Object:
    
    @staticmethod
    def dimensions(dimensions_list:list):
        dimension_obj = [Dimension(name=dimension) for dimension in dimensions_list]
        return dimension_obj
    
    @staticmethod
    def metrics(metrics_list:list):
        metrics_obj = [Metric(name=metric) for metric in metrics_list]
        return metrics_obj   

class GA4Filter:

    @staticmethod
    def filter(field:str,value:str):
        filter_obj = FilterExpression(
            filter=Filter(
                field_name=field,
                string_filter=Filter.StringFilter(value=value),
                        )
                            )
        return filter_obj
    
    @staticmethod
    def get_filter_list(filter_dict:dict):
        filter_list = []
        for key,value in filter_dict.items():
            filter_item = FilterExpression(
                        filter=Filter(
                            field_name=key,
                            string_filter=Filter.StringFilter(value=value),
                        )
            )
            filter_list.append(filter_item)
        return filter_list
    
    @staticmethod
    def filter_andGroup(filter_list:list):
        filter_obj = FilterExpression(
            and_group=FilterExpressionList(
                expressions=filter_list
            )
        )
        return filter_obj
    
    @staticmethod
    def filter_OrGroup(filter_list:list):
        filter_obj = FilterExpression(
            or_group=FilterExpressionList(
                expressions=filter_list
            )
        )
        return filter_obj

    @staticmethod
    def filter_notexpression_single(field:str,value:str):
        filter_obj = FilterExpression(
            not_expression=FilterExpression(
                filter=Filter(
                    field_name=field,
                    string_filter=Filter.StringFilter(value=value),
                )
            )
        )
        return filter_obj
    
    @staticmethod
    def filter_notexpression_list(filter_list:list):
        filter_obj = FilterExpression(
            not_expression=FilterExpressionList(
                expressions=filter_list
            )
        )
        return filter_obj
    
    @staticmethod
    def filter_inlist(field:str,value_list:list):
        filter_obj = FilterExpression(
            filter=Filter(
                field_name=field,
                in_list_filter=Filter.InListFilter(
                    values=value_list
                ),
            )
        )
        return filter_obj

class GetGA4Data:

    @staticmethod
    def getData(propertyId:str,dimensions:list,metrics:list,start_date:str,end_date:str,filter_obj:FilterExpression=None):
        client = BetaAnalyticsDataClient()

        request = RunReportRequest(
            property=f"properties/{propertyId}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=filter_obj
        )
        response = client.run_report(request)
        return response

def ga4_response_to_list_of_dicts(response:RunReportResponse,dimensions:list,metrics:list) -> list:
    temp_list = []
    for row in response.rows:
        temp_dict = {}
        dimension_index = 0
        metric_index = 0
        for dimension in dimensions:
            temp_dict.update({dimension:row.dimension_values[dimension_index].value})
            dimension_index += 1
        for metric in metrics:
            temp_dict.update({metric:row.metric_values[metric_index].value})
            metric_index += 1
        
        temp_list.append(temp_dict)
    
    return temp_list