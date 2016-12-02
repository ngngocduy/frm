using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CheckThuaDat_Phuluchopdongtangdientich
{
    public class CheckThuaDat_Phuluchopdongtangdientich :IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_thuadat"))
            {
                StringBuilder fetchXml = new StringBuilder();
                fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                fetchXml.Append("<entity name='new_thuadatcanhtac'>");
                fetchXml.Append("<attribute name='new_thuadatcanhtacid'/>");
                fetchXml.Append("<filter type='and'>");
                fetchXml.Append(string.Format("<condition attribute='new_thuadat' operator='eq' value='{0}'></condition>", ((EntityReference)target["new_thuadat"]).Id));
                fetchXml.Append(string.Format("<condition attribute='statuscode' operator='eq' value='{0}'></condition>", 100000000));
                fetchXml.Append("</filter>");
                fetchXml.Append("</entity>");
                fetchXml.Append("</fetch>");

                EntityCollection entc = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                if (entc.Entities.Count > 0)
                {
                    throw new Exception("Thửa đất đã tồn tại trong chi tiết hợp đồng đầu tư mía khác !!!");
                }
            }
        }
    }
}
