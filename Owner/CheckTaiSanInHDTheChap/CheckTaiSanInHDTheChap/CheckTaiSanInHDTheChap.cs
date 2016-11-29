using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CheckTaiSanInHDTheChap
{
    public class CheckTaiSanInHDTheChap : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {

            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            int count = 0;
            
            
            if (context.MessageName == "Update" || context.MessageName == "Create")
            {
                if (target.Contains("new_taisan"))
                {
                    Entity taisanthechap = service.Retrieve(target.LogicalName, target.Id,
                        new ColumnSet(new string[] { "new_taisan", "new_hopdongthechap", "new_name" }));
                    
                    List<Entity> lstTaisanthechap = RetrieveMultiRecord(service, "new_taisanthechap",
                        new ColumnSet(new string[] { "new_taisanthechapid", "new_taisan", "new_name" }), "new_hopdongthechap", ((EntityReference)taisanthechap["new_hopdongthechap"]).Id);
                    
                    //throw new Exception(lstTaisanthechap[0]["new_name"].ToString());
                    foreach (Entity en in lstTaisanthechap)
                    {
                        if (!en.Contains("new_taisan") || !taisanthechap.Contains("new_taisan"))
                        {
                            continue;
                        }
                        if (((EntityReference)en["new_taisan"]).Id == ((EntityReference)taisanthechap["new_taisan"]).Id)
                        {
                            count++;
                        }
                    }
                    
                    if (count > 1)
                    {
                        throw new Exception("Tàn sản đã tồn tại");
                    }
                }
            }
        }

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }
    }
}
