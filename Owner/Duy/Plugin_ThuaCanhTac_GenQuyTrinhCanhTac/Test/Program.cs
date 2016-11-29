using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Discovery;
using Microsoft.Xrm.Sdk.XmlNamespaces;
using Microsoft.Xrm.Client.Services;
using System.Configuration;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;



namespace Test
{
    class Program
    {
        private IOrganizationServiceFactory factory;
        private IOrganizationService service;
        public Entity Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            Entity target = context.InputParameters["Target"] as Entity;
            return target;
            if (target.LogicalName == "new_thuadatcanhtac")
            {
                if (context.MessageName == "Create")
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);
                    //Gen(target);
                }
                else if (context.MessageName == "Update")
                {
                    bool flag = false;

                    string[] arr = new string[] { "new_vutrong", "new_hopdongdautumia", "new_loaigocmia", "new_giongmia", "new_luugoc", "new_tuoimia", "new_mucdichsanxuatmia", "new_thuadat" };

                    foreach (string temp in arr)
                    {
                        if (target.Contains(temp))
                        {
                            flag = true;
                            break;
                        }
                    }


                    if (flag == true)
                    {
                        factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                        service = factory.CreateOrganizationService(context.UserId);
                        Entity chitiethddtm = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                        //Gen(chitiethddtm);
                    }
                }
            }
            
        }

        private void Gen(Entity target)
        {
            if (!target.Attributes.Contains("new_hopdongdautumia"))
                throw new Exception("Vui lòng chọn hợp đồng đầu tư mía!");
            EntityReference new_hopdongdautumia = (EntityReference)target["new_hopdongdautumia"];
            Entity hopdongdautumia = service.Retrieve(
                new_hopdongdautumia.LogicalName,
                new_hopdongdautumia.Id,
                new ColumnSet(new string[]{
                        "new_vudautu",
                        "new_khachhang",
                        "new_khachhangdoanhnghiep"
                    }));
            if (hopdongdautumia == null)
                throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa", new_hopdongdautumia.Name));
            if (!hopdongdautumia.Attributes.Contains("new_vudautu"))
                throw new Exception(string.Format("Vui lòng chọn mùa vụ trong hợp đồng đầu tư mía '{0}'", new_hopdongdautumia.Name));
            EntityReference new_vudautu = (EntityReference)hopdongdautumia["new_vudautu"];
            EntityReference new_khachhang = null;
            if (!target.Attributes.Contains("new_khachhang"))
            {
                if (!target.Attributes.Contains("new_khachhangdoanhnghiep"))
                    throw new Exception("Vui lòng chọn khách hàng!");
                else
                {
                    if (!hopdongdautumia.Attributes.Contains("new_khachhangdoanhnghiep"))
                        throw new Exception(string.Format("Vui lòng chọn khách hàng trên hợp đồng đầu tư '{0}'", new_hopdongdautumia.Name));
                    else if (((EntityReference)target["new_khachhangdoanhnghiep"]).Id.ToString() != ((EntityReference)hopdongdautumia["new_khachhangdoanhnghiep"]).Id.ToString())
                        throw new Exception(string.Format("Khách hàng trên hợp đồng đầu tư mía '{0}' và khách hàng trên hợp đồng đầu tư chi tiết không khớp. Vui lòng kiểm tra lại!", new_hopdongdautumia.Name));
                    else
                        new_khachhang = (EntityReference)hopdongdautumia["new_khachhangdoanhnghiep"];
                }
            }
            else
            {
                if (!hopdongdautumia.Attributes.Contains("new_khachhang"))
                    throw new Exception(string.Format("Vui lòng chọn khách hàng trên hợp đồng đầu tư '{0}'!", new_hopdongdautumia.Name));
                else if (((EntityReference)target["new_khachhang"]).Id.ToString() != ((EntityReference)hopdongdautumia["new_khachhang"]).Id.ToString())
                    throw new Exception(string.Format("Khách hàng trên hợp đồng đầu tư mía '{0}' và khách hàng trên thửa canh tác không khớp. Vui lòng kiểm tra lại!", new_hopdongdautumia.Name));
                else
                    new_khachhang = (EntityReference)hopdongdautumia["new_khachhang"];
            }

            if (!target.Attributes.Contains("new_thuadat"))
                throw new Exception("Vui lòng chọn thửa đất!");
            EntityReference new_thuadat = (EntityReference)target["new_thuadat"];
            Entity thuadat = service.Retrieve(new_thuadat.LogicalName, new_thuadat.Id, new ColumnSet(new string[]{
                    "new_nhomdat","new_loaidat"
                }));
            if (thuadat == null)
                throw new Exception(string.Format("Thửa đất '{0}' không tồn tại hoặc đã bị xóa!", new_thuadat.Name));
            if (!thuadat.Attributes.Contains("new_nhomdat"))
                throw new Exception(string.Format("Vui lòng chọn nhóm đất tại thửa đất '{0}", new_thuadat.Name));
            string new_nhomdat = ((OptionSetValue)thuadat["new_nhomdat"]).Value.ToString();

            if (!thuadat.Attributes.Contains("new_loaidat"))
            {
                throw new Exception(string.Format("Vui lòng chọn loại đất tại thửa đất '{0}'", new_thuadat.Name));
            }
            string new_loaidat = ((OptionSetValue)thuadat["new_loaidat"]).Value.ToString();

            if (!target.Attributes.Contains("new_vutrong"))
                throw new Exception("Vui lòng chọn vụ trồng!");
            int new_vutrong = ((OptionSetValue)target["new_vutrong"]).Value;
            if (!target.Attributes.Contains("new_loaigocmia"))
                throw new Exception("Vui lòng chọn loại gốc mía!");
            string new_loaigocmia = ((OptionSetValue)target["new_loaigocmia"]).Value.ToString();
            if (!target.Attributes.Contains("new_giongmia"))
                throw new Exception("Vui lòng chọn giống mía!");
            EntityReference new_giongmia = (EntityReference)target["new_giongmia"];

            if (!target.Contains("new_luugoc"))
                throw new Exception("Vui lòng chọn lưu gốc!");
            int new_luugoc = (int)target["new_luugoc"];

            if (!target.Contains("new_tuoimia"))
                throw new Exception("Vui lòng chọn tưới mía!");
            int tuoimia = (int)target["new_tuoimia"];

            if (!target.Contains("new_mucdichsanxuatmia"))
                throw new Exception("Vui lòng chọn mục đích sản xuất!");
            string new_mucdichsanxuatmia = ((OptionSetValue)target["new_mucdichsanxuatmia"]).Value.ToString();

            Entity giongmia = service.Retrieve(new_giongmia.LogicalName, new_giongmia.Id, new ColumnSet(new string[] {
                    "new_vutrong",
                    //"new_loaigocmia",
                    "new_tuoichinmiagoc",
                    "new_khuyencaodattrong","new_nhomgiong","new_tuoichinmiato"}));
            if (giongmia == null)
                throw new Exception(string.Format("Giống mía '{0}' không tồn tại hoặc đã bị xóa!", new_giongmia.Name));
            if (!giongmia.Attributes.Contains("new_nhomgiong"))
                throw new Exception(string.Format("Vui Lòng chọn nhóm giống tại giống mía '{0}'!", new_giongmia.Name));
            string nhomgiong = ((OptionSetValue)giongmia["new_nhomgiong"]).Value.ToString();
            if (!giongmia.Attributes.Contains("new_tuoichinmiato"))
                throw new Exception(string.Format("Vui lòng nhập 'tuổi chín mía tơ' tại giống mia '{0}'!", new_giongmia.Name));
            int tuoichinmiato = (int)giongmia["new_tuoichinmiato"];
            if (!giongmia.Attributes.Contains("new_tuoichinmiagoc"))
                throw new Exception(string.Format("Vui lòng nhập 'tuổi chín mía gốc' tại giống mia '{0}'!", new_giongmia.Name));
            int tuoichinmiagoc = (int)giongmia["new_tuoichinmiagoc"];


            if (!target.Attributes.Contains("new_ngaytrong"))
                throw new Exception("Vui lòng chọn ngày trồng!");
            DateTime new_ngaytrong = (DateTime)target["new_ngaytrong"];

            QueryExpression q = new QueryExpression("new_quitrinhcanhtac");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression(LogicalOperator.And);
            LinkEntity linkgiongmia = new LinkEntity("new_quitrinhcanhtac", "new_new_quitrinhcanhtac_new_giongmia", "new_quitrinhcanhtacid", "new_quitrinhcanhtacid", JoinOperator.Inner);

            q.LinkEntities.Add(linkgiongmia);
            linkgiongmia.LinkCriteria = new FilterExpression();
            linkgiongmia.LinkCriteria.AddCondition("new_giongmiaid", ConditionOperator.Equal, giongmia.Id);
            //throw new Exception(giongmia.Id.ToString() + tuoimia.ToString() + nhomgiong.ToString() + new_nhomdat.ToString() + new_loaidat.ToString() + new_vutrong.ToString() + new_mucdichsanxuatmia.ToString() + new_loaigocmia.ToString());
            q.Criteria.AddCondition(new ConditionExpression("new_hidetuoimia", ConditionOperator.Like, "%" + tuoimia.ToString() + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hidenhomgiongmia", ConditionOperator.Like, "%" + nhomgiong + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hidenhomdat", ConditionOperator.Like, "%" + new_nhomdat + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_loaidat_vl", ConditionOperator.Like, "%" + new_loaidat + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hidevutrong", ConditionOperator.Like, "%" + new_vutrong.ToString() + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hidemucdichsanxuatmia", ConditionOperator.Like, "%" + new_mucdichsanxuatmia + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hideloaigocmia", ConditionOperator.Like, "%" + new_loaigocmia + "%"));//new_hidetuoimia
            q.TopCount = 1;
            Entity quytrinhcanhtac = null;
            EntityCollection entc = service.RetrieveMultiple(q);
            if (entc.Entities.Count() <= 0)
            {
                q = new QueryExpression("new_quitrinhcanhtac");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression(LogicalOperator.And);
                q.Criteria.AddCondition(new ConditionExpression("new_macdinh", ConditionOperator.Equal, true));
                q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                q.TopCount = 1;
                entc = service.RetrieveMultiple(q);
                if (entc.Entities.Count() <= 0)
                    throw new Exception("Không tồn tại quy trình canh tác nào tương ứng với dữ kiện bạn cung cấp. Vui lòng tạo quy trình tương ứng hoặc tạo quy trình mặc định!");
                else
                    quytrinhcanhtac = entc.Entities[0];
            }
            else
                quytrinhcanhtac = entc.Entities[0];


            StringBuilder fetch = new StringBuilder();

            fetch.AppendLine("<?xml version='1.0' encoding='utf-8'?>");
            fetch.AppendLine("<fetch mapping='logical' aggregate='true'>");
            fetch.AppendLine("<entity name='new_new_quitrinhcanhtac_new_vudautu'>");
            fetch.AppendLine("<attribute name='new_new_quitrinhcanhtac_new_vudautuid' aggregate='count' alias='count'/>");
            fetch.AppendLine("<filter type='and'>");
            fetch.AppendLine("<condition attribute='new_quitrinhcanhtacid' operator='eq' value='" + quytrinhcanhtac.Id.ToString() + "'></condition>");
            fetch.AppendLine("<condition attribute='new_vudautuid' operator='eq' value='" + new_vudautu.Id.ToString() + "'></condition>");
            fetch.AppendLine("</filter>");
            fetch.AppendLine("<link-entity name='new_quitrinhcanhtac' from='new_quitrinhcanhtacid' to='new_quitrinhcanhtacid' link-type='inner'>");
            fetch.AppendLine("<filter type='and'>");
            fetch.AppendLine("<condition attribute='statecode' operator='eq' value='0'></condition>");
            fetch.AppendLine("</filter>");
            fetch.AppendLine("</link-entity>");
            fetch.AppendLine("</entity>");
            fetch.AppendLine("</fetch>");
            EntityCollection eCount = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            int count = 0;
            if (eCount.Entities.Count() <= 0)
                count = 0;
            else
            {
                Entity tmp = eCount.Entities[0];
                if (!tmp.Attributes.Contains("count"))
                    count = 0;
                else
                    count = (int)((AliasedValue)tmp["count"]).Value;

                Entity a = new Entity(target.LogicalName);
                a.Id = target.Id;
                a["new_quitrinhcanhtac"] = quytrinhcanhtac.ToEntityReference();
                service.Update(a);
            }
            if (count <= 0)
                throw new Exception(string.Format("Vụ đầu tư '{0}' chưa có quy trình canh tác '{1}'!", new_vudautu.Name, quytrinhcanhtac["new_name"]));

            QueryExpression q1 = new QueryExpression("new_quitrinhcanhtacchitiet");
            q1.ColumnSet = new ColumnSet(new string[] {
                    "new_name",
                    "new_hangmuccanhtac",
                    "new_songaysaukhitrong",
                    "new_quitrinhcanhtac",
                    "new_sothoigianthuchien",
                    "new_lanthuchien"
                });
            q1.Orders.Add(new OrderExpression("new_songaysaukhitrong", OrderType.Ascending));
            q1.Criteria = new FilterExpression(LogicalOperator.And);
            q1.Criteria.AddCondition(new ConditionExpression("new_quitrinhcanhtac", ConditionOperator.Equal, quytrinhcanhtac.Id));
            EntityCollection qtcs = service.RetrieveMultiple(q1);
            if (qtcs.Entities.Count() <= 0)
                throw new Exception(string.Format("Quy trình canh tác '{0}' chưa có quy trình canh tác chi tiết. Vui lòng thêm quy trình canh tác chi tiết!", quytrinhcanhtac["new_name"].ToString()));
            //throw new Exception(qtcs.Entities.Count() + "");
            foreach (Entity qtc in qtcs.Entities)
            {
                string qtctctName = qtc.Attributes.Contains("new_name") ? "'" + qtc["new_name"] + "'" : "";
                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                    throw new Exception(string.Format("Vui lòng chọn hạng mục canh tác trên Quy trình canh tác chi tiết {0}!", qtctctName));
                EntityReference hmRef = qtc["new_hangmuccanhtac"] as EntityReference;
                Entity hm = service.Retrieve(hmRef.LogicalName, hmRef.Id, new ColumnSet(new string[] { "new_name", "new_loaihangmuc", "new_yeucau" }));
                if (hm == null)
                    throw new Exception(string.Format("Hạng mục canh tác '{0}' trên quy trình canh tác chi tiết '{1}' không tồ tại hoặc bị xóa!", hmRef.Name, qtctctName));
                if (!hm.Attributes.Contains("new_loaihangmuc"))
                    throw new Exception(string.Format("Vui lòng chọn loại hạng mục canh tác trên hạng muc canh tác '{0}'!", hmRef.Name));
                int type = ((OptionSetValue)hm["new_loaihangmuc"]).Value;

                switch (type)
                {
                    case 100000001://Trồng mía
                        {
                            Entity en = new Entity("new_trongmia");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            en["subject"] = qtc["new_name"];
                            en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            en["new_hopdongtrongmia"] = new_hopdongdautumia;
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            en["new_hangmucanhtac"] = qtc["new_hangmuccanhtac"];
                            if (!qtc.Attributes.Contains("new_lanthuchien"))
                                throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            //en["new_lanbon"] = qtc["new_lanthuchien"];
                            en["new_vutrong"] = new OptionSetValue(new_vutrong);

                            //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                            //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình '{1}'", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_ngaytrongxulygoc"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            //if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //    en["new_ngaytrongxulygoc"] = qtc["new_sothoigianthuchien"];
                            en["regardingobjectid"] = target.ToEntityReference();
                            service.Create(en);
                        }
                        break;
                    case 100000002://Bón phân
                        {
                            Entity bp = new Entity("new_bonphan");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            bp["subject"] = qtc["new_name"];
                            bp["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            bp["new_hopdongtrongmia"] = new_hopdongdautumia;
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            bp["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                            if (!qtc.Attributes.Contains("new_lanthuchien"))
                                throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            bp["new_lanbon"] = qtc["new_lanthuchien"];

                            //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName,quytrinhcanhtac["new_name"]));
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                bp["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            bp["new_ngaybondukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                bp["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                            bp["regardingobjectid"] = target.ToEntityReference();
                            service.Create(bp);
                        }
                        break;
                    case 100000003://Xử lý cỏ dại
                        {
                            //new_xulycodai
                            Entity en = new Entity("new_xulycodai");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập Tên QT canh tác chi tiết của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            en["subject"] = qtc["new_name"];
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                            en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            en["new_hopdongtrongmia"] = new_hopdongdautumia;
                            if (!qtc.Attributes.Contains("new_lanthuchien"))
                                throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_lanxuly"] = qtc["new_lanthuchien"];

                            //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_ngayxulydukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                            en["regardingobjectid"] = target.ToEntityReference();
                            service.Create(en);
                        }
                        break;
                    case 100000006://Tưới mía
                        {
                            Entity en = new Entity("new_tuoimia");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}' của quy trình canh tac '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["subject"] = qtc["new_name"];
                            en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            en["new_hopdongtrongmia"] = new_hopdongdautumia;
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                            if (!qtc.Attributes.Contains("new_lanthuchien"))
                                throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_solantuoi"] = qtc["new_lanthuchien"];

                            //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                            //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_ngaytuoidukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                en["new_thoigiantuoi"] = qtc["new_sothoigianthuchien"];
                            en["regardingobjectid"] = target.ToEntityReference();
                            service.Create(en);
                        }
                        break;
                    case 100000004: //Xử lý sâu bệnh 
                        {
                            {
                                Entity en = new Entity("new_xulysaubenh");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}' của quy trình canh tac '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["subject"] = qtc["new_name"];
                                en["new_chitiethddtmia"] = new EntityReference(target.LogicalName, target.Id);
                                en["new_hopdongdautumia"] = new_hopdongdautumia;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_lanxuly"] = qtc["new_lanthuchien"];

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                                //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_ngayxulydukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    en["new_ngayxulythucte"] = qtc["new_sothoigianthuchien"];
                                en["regardingobjectid"] = target.ToEntityReference();
                                service.Create(en);
                            }
                        }
                        break;
                    case 100000007:
                    case 100000005:
                    case 100000008:
                    case 100000009:
                    case 100000000:
                    default:
                        //100000007:Khai mương chống úng" Khai mương chống úng
                        //100000007:Bóc lột lá mía || 100000008:khach || 100000009:San lấp mặt bằng
                        //100000000:Cày
                        {
                            Entity nk = new Entity("new_nhatkydongruong");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            nk["subject"] = qtc["new_name"];
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            nk["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                            nk["new_hopdongdautumia"] = new_hopdongdautumia;
                            nk["new_thuadatcanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            if (new_khachhang.LogicalName == "contact")
                                nk["new_khachhang"] = new_khachhang;
                            else if (new_khachhang.LogicalName == "account")
                                nk["new_khachhangdoanhnghiep"] = new_khachhang;
                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            nk["new_dukienthuchien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                nk["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                            nk["regardingobjectid"] = target.ToEntityReference();
                            service.Create(nk);
                        }
                        break;
                }
            }
        }

        private void Delete()
        {

        }

        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }

        static void Main(string[] args)
        {
            IServiceProvider serviceProvider;
            IOrganizationService service;
            //IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            //Entity target = context.InputParameters["Target"] as Entity;

            CRMConnector crm = new CRMConnector();
            crm.ConnectToCrm();

            Entity target = Execute(serviceProvider);

            if (!target.Attributes.Contains("new_hopdongdautumia"))
                throw new Exception("Vui lòng chọn hợp đồng đầu tư mía!");
            EntityReference new_hopdongdautumia = (EntityReference)target["new_hopdongdautumia"];
            Entity hopdongdautumia = service.Retrieve(
                new_hopdongdautumia.LogicalName,
                new_hopdongdautumia.Id,
                new ColumnSet(new string[]{
                        "new_vudautu",
                        "new_khachhang",
                        "new_khachhangdoanhnghiep"
                    }));
            if (hopdongdautumia == null)
                throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa", new_hopdongdautumia.Name));
            if (!hopdongdautumia.Attributes.Contains("new_vudautu"))
                throw new Exception(string.Format("Vui lòng chọn mùa vụ trong hợp đồng đầu tư mía '{0}'", new_hopdongdautumia.Name));
            EntityReference new_vudautu = (EntityReference)hopdongdautumia["new_vudautu"];
            EntityReference new_khachhang = null;
            if (!target.Attributes.Contains("new_khachhang"))
            {
                if (!target.Attributes.Contains("new_khachhangdoanhnghiep"))
                    throw new Exception("Vui lòng chọn khách hàng!");
                else
                {
                    if (!hopdongdautumia.Attributes.Contains("new_khachhangdoanhnghiep"))
                        throw new Exception(string.Format("Vui lòng chọn khách hàng trên hợp đồng đầu tư '{0}'", new_hopdongdautumia.Name));
                    else if (((EntityReference)target["new_khachhangdoanhnghiep"]).Id.ToString() != ((EntityReference)hopdongdautumia["new_khachhangdoanhnghiep"]).Id.ToString())
                        throw new Exception(string.Format("Khách hàng trên hợp đồng đầu tư mía '{0}' và khách hàng trên hợp đồng đầu tư chi tiết không khớp. Vui lòng kiểm tra lại!", new_hopdongdautumia.Name));
                    else
                        new_khachhang = (EntityReference)hopdongdautumia["new_khachhangdoanhnghiep"];
                }
            }
            else
            {
                if (!hopdongdautumia.Attributes.Contains("new_khachhang"))
                    throw new Exception(string.Format("Vui lòng chọn khách hàng trên hợp đồng đầu tư '{0}'!", new_hopdongdautumia.Name));
                else if (((EntityReference)target["new_khachhang"]).Id.ToString() != ((EntityReference)hopdongdautumia["new_khachhang"]).Id.ToString())
                    throw new Exception(string.Format("Khách hàng trên hợp đồng đầu tư mía '{0}' và khách hàng trên thửa canh tác không khớp. Vui lòng kiểm tra lại!", new_hopdongdautumia.Name));
                else
                    new_khachhang = (EntityReference)hopdongdautumia["new_khachhang"];
            }

            if (!target.Attributes.Contains("new_thuadat"))
                throw new Exception("Vui lòng chọn thửa đất!");
            EntityReference new_thuadat = (EntityReference)target["new_thuadat"];
            Entity thuadat = service.Retrieve(new_thuadat.LogicalName, new_thuadat.Id, new ColumnSet(new string[]{
                    "new_nhomdat","new_loaidat"
                }));
            if (thuadat == null)
                throw new Exception(string.Format("Thửa đất '{0}' không tồn tại hoặc đã bị xóa!", new_thuadat.Name));
            if (!thuadat.Attributes.Contains("new_nhomdat"))
                throw new Exception(string.Format("Vui lòng chọn nhóm đất tại thửa đất '{0}", new_thuadat.Name));
            string new_nhomdat = ((OptionSetValue)thuadat["new_nhomdat"]).Value.ToString();

            if (!thuadat.Attributes.Contains("new_loaidat"))
            {
                throw new Exception(string.Format("Vui lòng chọn loại đất tại thửa đất '{0}'", new_thuadat.Name));
            }
            string new_loaidat = ((OptionSetValue)thuadat["new_loaidat"]).Value.ToString();

            if (!target.Attributes.Contains("new_vutrong"))
                throw new Exception("Vui lòng chọn vụ trồng!");
            int new_vutrong = ((OptionSetValue)target["new_vutrong"]).Value;
            if (!target.Attributes.Contains("new_loaigocmia"))
                throw new Exception("Vui lòng chọn loại gốc mía!");
            string new_loaigocmia = ((OptionSetValue)target["new_loaigocmia"]).Value.ToString();
            if (!target.Attributes.Contains("new_giongmia"))
                throw new Exception("Vui lòng chọn giống mía!");
            EntityReference new_giongmia = (EntityReference)target["new_giongmia"];

            if (!target.Contains("new_luugoc"))
                throw new Exception("Vui lòng chọn lưu gốc!");
            int new_luugoc = (int)target["new_luugoc"];

            if (!target.Contains("new_tuoimia"))
                throw new Exception("Vui lòng chọn tưới mía!");
            int tuoimia = (int)target["new_tuoimia"];

            if (!target.Contains("new_mucdichsanxuatmia"))
                throw new Exception("Vui lòng chọn mục đích sản xuất!");
            string new_mucdichsanxuatmia = ((OptionSetValue)target["new_mucdichsanxuatmia"]).Value.ToString();

            Entity giongmia = service.Retrieve(new_giongmia.LogicalName, new_giongmia.Id, new ColumnSet(new string[] {
                    "new_vutrong",
                    //"new_loaigocmia",
                    "new_tuoichinmiagoc",
                    "new_khuyencaodattrong","new_nhomgiong","new_tuoichinmiato"}));
            if (giongmia == null)
                throw new Exception(string.Format("Giống mía '{0}' không tồn tại hoặc đã bị xóa!", new_giongmia.Name));
            if (!giongmia.Attributes.Contains("new_nhomgiong"))
                throw new Exception(string.Format("Vui Lòng chọn nhóm giống tại giống mía '{0}'!", new_giongmia.Name));
            string nhomgiong = ((OptionSetValue)giongmia["new_nhomgiong"]).Value.ToString();
            if (!giongmia.Attributes.Contains("new_tuoichinmiato"))
                throw new Exception(string.Format("Vui lòng nhập 'tuổi chín mía tơ' tại giống mia '{0}'!", new_giongmia.Name));
            int tuoichinmiato = (int)giongmia["new_tuoichinmiato"];
            if (!giongmia.Attributes.Contains("new_tuoichinmiagoc"))
                throw new Exception(string.Format("Vui lòng nhập 'tuổi chín mía gốc' tại giống mia '{0}'!", new_giongmia.Name));
            int tuoichinmiagoc = (int)giongmia["new_tuoichinmiagoc"];


            if (!target.Attributes.Contains("new_ngaytrong"))
                throw new Exception("Vui lòng chọn ngày trồng!");
            DateTime new_ngaytrong = (DateTime)target["new_ngaytrong"];

            QueryExpression q = new QueryExpression("new_quitrinhcanhtac");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression(LogicalOperator.And);
            LinkEntity linkgiongmia = new LinkEntity("new_quitrinhcanhtac", "new_new_quitrinhcanhtac_new_giongmia", "new_quitrinhcanhtacid", "new_quitrinhcanhtacid", JoinOperator.Inner);

            q.LinkEntities.Add(linkgiongmia);
            linkgiongmia.LinkCriteria = new FilterExpression();
            linkgiongmia.LinkCriteria.AddCondition("new_giongmiaid", ConditionOperator.Equal, giongmia.Id);
            //throw new Exception(giongmia.Id.ToString() + tuoimia.ToString() + nhomgiong.ToString() + new_nhomdat.ToString() + new_loaidat.ToString() + new_vutrong.ToString() + new_mucdichsanxuatmia.ToString() + new_loaigocmia.ToString());
            q.Criteria.AddCondition(new ConditionExpression("new_hidetuoimia", ConditionOperator.Like, "%" + tuoimia.ToString() + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hidenhomgiongmia", ConditionOperator.Like, "%" + nhomgiong + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hidenhomdat", ConditionOperator.Like, "%" + new_nhomdat + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_loaidat_vl", ConditionOperator.Like, "%" + new_loaidat + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hidevutrong", ConditionOperator.Like, "%" + new_vutrong.ToString() + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hidemucdichsanxuatmia", ConditionOperator.Like, "%" + new_mucdichsanxuatmia + "%"));
            q.Criteria.AddCondition(new ConditionExpression("new_hideloaigocmia", ConditionOperator.Like, "%" + new_loaigocmia + "%"));//new_hidetuoimia
            q.TopCount = 1;
            Entity quytrinhcanhtac = null;
            EntityCollection entc = service.RetrieveMultiple(q);
            if (entc.Entities.Count() <= 0)
            {
                q = new QueryExpression("new_quitrinhcanhtac");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression(LogicalOperator.And);
                q.Criteria.AddCondition(new ConditionExpression("new_macdinh", ConditionOperator.Equal, true));
                q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                q.TopCount = 1;
                entc = service.RetrieveMultiple(q);
                if (entc.Entities.Count() <= 0)
                    throw new Exception("Không tồn tại quy trình canh tác nào tương ứng với dữ kiện bạn cung cấp. Vui lòng tạo quy trình tương ứng hoặc tạo quy trình mặc định!");
                else
                    quytrinhcanhtac = entc.Entities[0];
            }
            else
                quytrinhcanhtac = entc.Entities[0];


            StringBuilder fetch = new StringBuilder();

            fetch.AppendLine("<?xml version='1.0' encoding='utf-8'?>");
            fetch.AppendLine("<fetch mapping='logical' aggregate='true'>");
            fetch.AppendLine("<entity name='new_new_quitrinhcanhtac_new_vudautu'>");
            fetch.AppendLine("<attribute name='new_new_quitrinhcanhtac_new_vudautuid' aggregate='count' alias='count'/>");
            fetch.AppendLine("<filter type='and'>");
            fetch.AppendLine("<condition attribute='new_quitrinhcanhtacid' operator='eq' value='" + quytrinhcanhtac.Id.ToString() + "'></condition>");
            fetch.AppendLine("<condition attribute='new_vudautuid' operator='eq' value='" + new_vudautu.Id.ToString() + "'></condition>");
            fetch.AppendLine("</filter>");
            fetch.AppendLine("<link-entity name='new_quitrinhcanhtac' from='new_quitrinhcanhtacid' to='new_quitrinhcanhtacid' link-type='inner'>");
            fetch.AppendLine("<filter type='and'>");
            fetch.AppendLine("<condition attribute='statecode' operator='eq' value='0'></condition>");
            fetch.AppendLine("</filter>");
            fetch.AppendLine("</link-entity>");
            fetch.AppendLine("</entity>");
            fetch.AppendLine("</fetch>");
            EntityCollection eCount = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            int count = 0;
            if (eCount.Entities.Count() <= 0)
                count = 0;
            else
            {
                Entity tmp = eCount.Entities[0];
                if (!tmp.Attributes.Contains("count"))
                    count = 0;
                else
                    count = (int)((AliasedValue)tmp["count"]).Value;

                Entity a = new Entity(target.LogicalName);
                a.Id = target.Id;
                a["new_quitrinhcanhtac"] = quytrinhcanhtac.ToEntityReference();
                service.Update(a);
            }
            if (count <= 0)
                throw new Exception(string.Format("Vụ đầu tư '{0}' chưa có quy trình canh tác '{1}'!", new_vudautu.Name, quytrinhcanhtac["new_name"]));

            QueryExpression q1 = new QueryExpression("new_quitrinhcanhtacchitiet");
            q1.ColumnSet = new ColumnSet(new string[] {
                    "new_name",
                    "new_hangmuccanhtac",
                    "new_songaysaukhitrong",
                    "new_quitrinhcanhtac",
                    "new_sothoigianthuchien",
                    "new_lanthuchien"
                });
            q1.Orders.Add(new OrderExpression("new_songaysaukhitrong", OrderType.Ascending));
            q1.Criteria = new FilterExpression(LogicalOperator.And);
            q1.Criteria.AddCondition(new ConditionExpression("new_quitrinhcanhtac", ConditionOperator.Equal, quytrinhcanhtac.Id));
            EntityCollection qtcs = service.RetrieveMultiple(q1);
            if (qtcs.Entities.Count() <= 0)
                throw new Exception(string.Format("Quy trình canh tác '{0}' chưa có quy trình canh tác chi tiết. Vui lòng thêm quy trình canh tác chi tiết!", quytrinhcanhtac["new_name"].ToString()));
            //throw new Exception(qtcs.Entities.Count() + "");
            foreach (Entity qtc in qtcs.Entities)
            {
                string qtctctName = qtc.Attributes.Contains("new_name") ? "'" + qtc["new_name"] + "'" : "";
                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                    throw new Exception(string.Format("Vui lòng chọn hạng mục canh tác trên Quy trình canh tác chi tiết {0}!", qtctctName));
                EntityReference hmRef = qtc["new_hangmuccanhtac"] as EntityReference;
                Entity hm = service.Retrieve(hmRef.LogicalName, hmRef.Id, new ColumnSet(new string[] { "new_name", "new_loaihangmuc", "new_yeucau" }));
                if (hm == null)
                    throw new Exception(string.Format("Hạng mục canh tác '{0}' trên quy trình canh tác chi tiết '{1}' không tồ tại hoặc bị xóa!", hmRef.Name, qtctctName));
                if (!hm.Attributes.Contains("new_loaihangmuc"))
                    throw new Exception(string.Format("Vui lòng chọn loại hạng mục canh tác trên hạng muc canh tác '{0}'!", hmRef.Name));
                int type = ((OptionSetValue)hm["new_loaihangmuc"]).Value;

                switch (type)
                {
                    case 100000001://Trồng mía
                        {
                            Entity en = new Entity("new_trongmia");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            en["subject"] = qtc["new_name"];
                            en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            en["new_hopdongtrongmia"] = new_hopdongdautumia;
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            en["new_hangmucanhtac"] = qtc["new_hangmuccanhtac"];
                            if (!qtc.Attributes.Contains("new_lanthuchien"))
                                throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            //en["new_lanbon"] = qtc["new_lanthuchien"];
                            en["new_vutrong"] = new OptionSetValue(new_vutrong);

                            //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                            //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình '{1}'", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_ngaytrongxulygoc"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            //if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //    en["new_ngaytrongxulygoc"] = qtc["new_sothoigianthuchien"];
                            en["regardingobjectid"] = target.ToEntityReference();
                            service.Create(en);
                        }
                        break;
                    case 100000002://Bón phân
                        {
                            Entity bp = new Entity("new_bonphan");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            bp["subject"] = qtc["new_name"];
                            bp["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            bp["new_hopdongtrongmia"] = new_hopdongdautumia;
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            bp["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                            if (!qtc.Attributes.Contains("new_lanthuchien"))
                                throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            bp["new_lanbon"] = qtc["new_lanthuchien"];

                            //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName,quytrinhcanhtac["new_name"]));
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                bp["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            bp["new_ngaybondukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                bp["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                            bp["regardingobjectid"] = target.ToEntityReference();
                            service.Create(bp);
                        }
                        break;
                    case 100000003://Xử lý cỏ dại
                        {
                            //new_xulycodai
                            Entity en = new Entity("new_xulycodai");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập Tên QT canh tác chi tiết của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            en["subject"] = qtc["new_name"];
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                            en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            en["new_hopdongtrongmia"] = new_hopdongdautumia;
                            if (!qtc.Attributes.Contains("new_lanthuchien"))
                                throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_lanxuly"] = qtc["new_lanthuchien"];

                            //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_ngayxulydukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                            en["regardingobjectid"] = target.ToEntityReference();
                            service.Create(en);
                        }
                        break;
                    case 100000006://Tưới mía
                        {
                            Entity en = new Entity("new_tuoimia");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}' của quy trình canh tac '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["subject"] = qtc["new_name"];
                            en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            en["new_hopdongtrongmia"] = new_hopdongdautumia;
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                            if (!qtc.Attributes.Contains("new_lanthuchien"))
                                throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_solantuoi"] = qtc["new_lanthuchien"];

                            //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                            //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                            //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                            en["new_ngaytuoidukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                en["new_thoigiantuoi"] = qtc["new_sothoigianthuchien"];
                            en["regardingobjectid"] = target.ToEntityReference();
                            service.Create(en);
                        }
                        break;
                    case 100000004: //Xử lý sâu bệnh 
                        {
                            {
                                Entity en = new Entity("new_xulysaubenh");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}' của quy trình canh tac '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["subject"] = qtc["new_name"];
                                en["new_chitiethddtmia"] = new EntityReference(target.LogicalName, target.Id);
                                en["new_hopdongdautumia"] = new_hopdongdautumia;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_lanxuly"] = qtc["new_lanthuchien"];

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                                //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_ngayxulydukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    en["new_ngayxulythucte"] = qtc["new_sothoigianthuchien"];
                                en["regardingobjectid"] = target.ToEntityReference();
                                service.Create(en);
                            }
                        }
                        break;
                    case 100000007:
                    case 100000005:
                    case 100000008:
                    case 100000009:
                    case 100000000:
                    default:
                        //100000007:Khai mương chống úng" Khai mương chống úng
                        //100000007:Bóc lột lá mía || 100000008:khach || 100000009:San lấp mặt bằng
                        //100000000:Cày
                        {
                            Entity nk = new Entity("new_nhatkydongruong");
                            if (!qtc.Attributes.Contains("new_name"))
                                throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                            nk["subject"] = qtc["new_name"];
                            if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            nk["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                            nk["new_hopdongdautumia"] = new_hopdongdautumia;
                            nk["new_thuadatcanhtac"] = new EntityReference(target.LogicalName, target.Id);
                            if (new_khachhang.LogicalName == "contact")
                                nk["new_khachhang"] = new_khachhang;
                            else if (new_khachhang.LogicalName == "account")
                                nk["new_khachhangdoanhnghiep"] = new_khachhang;
                            if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                            nk["new_dukienthuchien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                            if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                nk["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                            nk["regardingobjectid"] = target.ToEntityReference();
                            service.Create(nk);
                        }
                        break;
                }
            }

            OrganizationService service;
            var connectionstring = GetWindowsIntegratedSecurityConnectionString();
            var serverConnection = new ServerConnection(connectionstring);

            using (service = new OrganizationService(serverConnection.CRMConnection))
            {
                if (!target.Attributes.Contains("new_hopdongdautumia"))
                    throw new Exception("Vui lòng chọn hợp đồng đầu tư mía!");
                EntityReference new_hopdongdautumia = (EntityReference)target["new_hopdongdautumia"];
                Entity hopdongdautumia = service.Retrieve(
                    new_hopdongdautumia.LogicalName,
                    new_hopdongdautumia.Id,
                    new ColumnSet(new string[]{
                        "new_vudautu",
                        "new_khachhang",
                        "new_khachhangdoanhnghiep"
                    }));
                if (hopdongdautumia == null)
                    throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa", new_hopdongdautumia.Name));
                if (!hopdongdautumia.Attributes.Contains("new_vudautu"))
                    throw new Exception(string.Format("Vui lòng chọn mùa vụ trong hợp đồng đầu tư mía '{0}'", new_hopdongdautumia.Name));
                EntityReference new_vudautu = (EntityReference)hopdongdautumia["new_vudautu"];
                EntityReference new_khachhang = null;
                if (!target.Attributes.Contains("new_khachhang"))
                {
                    if (!target.Attributes.Contains("new_khachhangdoanhnghiep"))
                        throw new Exception("Vui lòng chọn khách hàng!");
                    else
                    {
                        if (!hopdongdautumia.Attributes.Contains("new_khachhangdoanhnghiep"))
                            throw new Exception(string.Format("Vui lòng chọn khách hàng trên hợp đồng đầu tư '{0}'", new_hopdongdautumia.Name));
                        else if (((EntityReference)target["new_khachhangdoanhnghiep"]).Id.ToString() != ((EntityReference)hopdongdautumia["new_khachhangdoanhnghiep"]).Id.ToString())
                            throw new Exception(string.Format("Khách hàng trên hợp đồng đầu tư mía '{0}' và khách hàng trên hợp đồng đầu tư chi tiết không khớp. Vui lòng kiểm tra lại!", new_hopdongdautumia.Name));
                        else
                            new_khachhang = (EntityReference)hopdongdautumia["new_khachhangdoanhnghiep"];
                    }
                }
                else
                {
                    if (!hopdongdautumia.Attributes.Contains("new_khachhang"))
                        throw new Exception(string.Format("Vui lòng chọn khách hàng trên hợp đồng đầu tư '{0}'!", new_hopdongdautumia.Name));
                    else if (((EntityReference)target["new_khachhang"]).Id.ToString() != ((EntityReference)hopdongdautumia["new_khachhang"]).Id.ToString())
                        throw new Exception(string.Format("Khách hàng trên hợp đồng đầu tư mía '{0}' và khách hàng trên thửa canh tác không khớp. Vui lòng kiểm tra lại!", new_hopdongdautumia.Name));
                    else
                        new_khachhang = (EntityReference)hopdongdautumia["new_khachhang"];
                }

                if (!target.Attributes.Contains("new_thuadat"))
                    throw new Exception("Vui lòng chọn thửa đất!");
                EntityReference new_thuadat = (EntityReference)target["new_thuadat"];
                Entity thuadat = service.Retrieve(new_thuadat.LogicalName, new_thuadat.Id, new ColumnSet(new string[]{
                    "new_nhomdat","new_loaidat"
                }));
                if (thuadat == null)
                    throw new Exception(string.Format("Thửa đất '{0}' không tồn tại hoặc đã bị xóa!", new_thuadat.Name));
                if (!thuadat.Attributes.Contains("new_nhomdat"))
                    throw new Exception(string.Format("Vui lòng chọn nhóm đất tại thửa đất '{0}", new_thuadat.Name));
                string new_nhomdat = ((OptionSetValue)thuadat["new_nhomdat"]).Value.ToString();

                if (!thuadat.Attributes.Contains("new_loaidat"))
                {
                    throw new Exception(string.Format("Vui lòng chọn loại đất tại thửa đất '{0}'", new_thuadat.Name));
                }
                string new_loaidat = ((OptionSetValue)thuadat["new_loaidat"]).Value.ToString();

                if (!target.Attributes.Contains("new_vutrong"))
                    throw new Exception("Vui lòng chọn vụ trồng!");
                int new_vutrong = ((OptionSetValue)target["new_vutrong"]).Value;
                if (!target.Attributes.Contains("new_loaigocmia"))
                    throw new Exception("Vui lòng chọn loại gốc mía!");
                string new_loaigocmia = ((OptionSetValue)target["new_loaigocmia"]).Value.ToString();
                if (!target.Attributes.Contains("new_giongmia"))
                    throw new Exception("Vui lòng chọn giống mía!");
                EntityReference new_giongmia = (EntityReference)target["new_giongmia"];

                if (!target.Contains("new_luugoc"))
                    throw new Exception("Vui lòng chọn lưu gốc!");
                int new_luugoc = (int)target["new_luugoc"];

                if (!target.Contains("new_tuoimia"))
                    throw new Exception("Vui lòng chọn tưới mía!");
                int tuoimia = (int)target["new_tuoimia"];

                if (!target.Contains("new_mucdichsanxuatmia"))
                    throw new Exception("Vui lòng chọn mục đích sản xuất!");
                string new_mucdichsanxuatmia = ((OptionSetValue)target["new_mucdichsanxuatmia"]).Value.ToString();

                Entity giongmia = service.Retrieve(new_giongmia.LogicalName, new_giongmia.Id, new ColumnSet(new string[] {
                    "new_vutrong",
                    //"new_loaigocmia",
                    "new_tuoichinmiagoc",
                    "new_khuyencaodattrong","new_nhomgiong","new_tuoichinmiato"}));
                if (giongmia == null)
                    throw new Exception(string.Format("Giống mía '{0}' không tồn tại hoặc đã bị xóa!", new_giongmia.Name));
                if (!giongmia.Attributes.Contains("new_nhomgiong"))
                    throw new Exception(string.Format("Vui Lòng chọn nhóm giống tại giống mía '{0}'!", new_giongmia.Name));
                string nhomgiong = ((OptionSetValue)giongmia["new_nhomgiong"]).Value.ToString();
                if (!giongmia.Attributes.Contains("new_tuoichinmiato"))
                    throw new Exception(string.Format("Vui lòng nhập 'tuổi chín mía tơ' tại giống mia '{0}'!", new_giongmia.Name));
                int tuoichinmiato = (int)giongmia["new_tuoichinmiato"];
                if (!giongmia.Attributes.Contains("new_tuoichinmiagoc"))
                    throw new Exception(string.Format("Vui lòng nhập 'tuổi chín mía gốc' tại giống mia '{0}'!", new_giongmia.Name));
                int tuoichinmiagoc = (int)giongmia["new_tuoichinmiagoc"];


                if (!target.Attributes.Contains("new_ngaytrong"))
                    throw new Exception("Vui lòng chọn ngày trồng!");
                DateTime new_ngaytrong = (DateTime)target["new_ngaytrong"];

                QueryExpression q = new QueryExpression("new_quitrinhcanhtac");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression(LogicalOperator.And);
                LinkEntity linkgiongmia = new LinkEntity("new_quitrinhcanhtac", "new_new_quitrinhcanhtac_new_giongmia", "new_quitrinhcanhtacid", "new_quitrinhcanhtacid", JoinOperator.Inner);

                q.LinkEntities.Add(linkgiongmia);
                linkgiongmia.LinkCriteria = new FilterExpression();
                linkgiongmia.LinkCriteria.AddCondition("new_giongmiaid", ConditionOperator.Equal, giongmia.Id);
                //throw new Exception(giongmia.Id.ToString() + tuoimia.ToString() + nhomgiong.ToString() + new_nhomdat.ToString() + new_loaidat.ToString() + new_vutrong.ToString() + new_mucdichsanxuatmia.ToString() + new_loaigocmia.ToString());
                q.Criteria.AddCondition(new ConditionExpression("new_hidetuoimia", ConditionOperator.Like, "%" + tuoimia.ToString() + "%"));
                q.Criteria.AddCondition(new ConditionExpression("new_hidenhomgiongmia", ConditionOperator.Like, "%" + nhomgiong + "%"));
                q.Criteria.AddCondition(new ConditionExpression("new_hidenhomdat", ConditionOperator.Like, "%" + new_nhomdat + "%"));
                q.Criteria.AddCondition(new ConditionExpression("new_loaidat_vl", ConditionOperator.Like, "%" + new_loaidat + "%"));
                q.Criteria.AddCondition(new ConditionExpression("new_hidevutrong", ConditionOperator.Like, "%" + new_vutrong.ToString() + "%"));
                q.Criteria.AddCondition(new ConditionExpression("new_hidemucdichsanxuatmia", ConditionOperator.Like, "%" + new_mucdichsanxuatmia + "%"));
                q.Criteria.AddCondition(new ConditionExpression("new_hideloaigocmia", ConditionOperator.Like, "%" + new_loaigocmia + "%"));//new_hidetuoimia
                q.TopCount = 1;
                Entity quytrinhcanhtac = null;
                EntityCollection entc = service.RetrieveMultiple(q);
                if (entc.Entities.Count() <= 0)
                {
                    q = new QueryExpression("new_quitrinhcanhtac");
                    q.ColumnSet = new ColumnSet(true);
                    q.Criteria = new FilterExpression(LogicalOperator.And);
                    q.Criteria.AddCondition(new ConditionExpression("new_macdinh", ConditionOperator.Equal, true));
                    q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                    q.TopCount = 1;
                    entc = service.RetrieveMultiple(q);
                    if (entc.Entities.Count() <= 0)
                        throw new Exception("Không tồn tại quy trình canh tác nào tương ứng với dữ kiện bạn cung cấp. Vui lòng tạo quy trình tương ứng hoặc tạo quy trình mặc định!");
                    else
                        quytrinhcanhtac = entc.Entities[0];
                }
                else
                    quytrinhcanhtac = entc.Entities[0];


                StringBuilder fetch = new StringBuilder();

                fetch.AppendLine("<?xml version='1.0' encoding='utf-8'?>");
                fetch.AppendLine("<fetch mapping='logical' aggregate='true'>");
                fetch.AppendLine("<entity name='new_new_quitrinhcanhtac_new_vudautu'>");
                fetch.AppendLine("<attribute name='new_new_quitrinhcanhtac_new_vudautuid' aggregate='count' alias='count'/>");
                fetch.AppendLine("<filter type='and'>");
                fetch.AppendLine("<condition attribute='new_quitrinhcanhtacid' operator='eq' value='" + quytrinhcanhtac.Id.ToString() + "'></condition>");
                fetch.AppendLine("<condition attribute='new_vudautuid' operator='eq' value='" + new_vudautu.Id.ToString() + "'></condition>");
                fetch.AppendLine("</filter>");
                fetch.AppendLine("<link-entity name='new_quitrinhcanhtac' from='new_quitrinhcanhtacid' to='new_quitrinhcanhtacid' link-type='inner'>");
                fetch.AppendLine("<filter type='and'>");
                fetch.AppendLine("<condition attribute='statecode' operator='eq' value='0'></condition>");
                fetch.AppendLine("</filter>");
                fetch.AppendLine("</link-entity>");
                fetch.AppendLine("</entity>");
                fetch.AppendLine("</fetch>");
                EntityCollection eCount = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
                int count = 0;
                if (eCount.Entities.Count() <= 0)
                    count = 0;
                else
                {
                    Entity tmp = eCount.Entities[0];
                    if (!tmp.Attributes.Contains("count"))
                        count = 0;
                    else
                        count = (int)((AliasedValue)tmp["count"]).Value;

                    Entity a = new Entity(target.LogicalName);
                    a.Id = target.Id;
                    a["new_quitrinhcanhtac"] = quytrinhcanhtac.ToEntityReference();
                    service.Update(a);
                }
                if (count <= 0)
                    throw new Exception(string.Format("Vụ đầu tư '{0}' chưa có quy trình canh tác '{1}'!", new_vudautu.Name, quytrinhcanhtac["new_name"]));

                QueryExpression q1 = new QueryExpression("new_quitrinhcanhtacchitiet");
                q1.ColumnSet = new ColumnSet(new string[] {
                    "new_name",
                    "new_hangmuccanhtac",
                    "new_songaysaukhitrong",
                    "new_quitrinhcanhtac",
                    "new_sothoigianthuchien",
                    "new_lanthuchien"
                });
                q1.Orders.Add(new OrderExpression("new_songaysaukhitrong", OrderType.Ascending));
                q1.Criteria = new FilterExpression(LogicalOperator.And);
                q1.Criteria.AddCondition(new ConditionExpression("new_quitrinhcanhtac", ConditionOperator.Equal, quytrinhcanhtac.Id));
                EntityCollection qtcs = service.RetrieveMultiple(q1);
                if (qtcs.Entities.Count() <= 0)
                    throw new Exception(string.Format("Quy trình canh tác '{0}' chưa có quy trình canh tác chi tiết. Vui lòng thêm quy trình canh tác chi tiết!", quytrinhcanhtac["new_name"].ToString()));
                //throw new Exception(qtcs.Entities.Count() + "");
                foreach (Entity qtc in qtcs.Entities)
                {
                    string qtctctName = qtc.Attributes.Contains("new_name") ? "'" + qtc["new_name"] + "'" : "";
                    if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                        throw new Exception(string.Format("Vui lòng chọn hạng mục canh tác trên Quy trình canh tác chi tiết {0}!", qtctctName));
                    EntityReference hmRef = qtc["new_hangmuccanhtac"] as EntityReference;
                    Entity hm = service.Retrieve(hmRef.LogicalName, hmRef.Id, new ColumnSet(new string[] { "new_name", "new_loaihangmuc", "new_yeucau" }));
                    if (hm == null)
                        throw new Exception(string.Format("Hạng mục canh tác '{0}' trên quy trình canh tác chi tiết '{1}' không tồ tại hoặc bị xóa!", hmRef.Name, qtctctName));
                    if (!hm.Attributes.Contains("new_loaihangmuc"))
                        throw new Exception(string.Format("Vui lòng chọn loại hạng mục canh tác trên hạng muc canh tác '{0}'!", hmRef.Name));
                    int type = ((OptionSetValue)hm["new_loaihangmuc"]).Value;

                    switch (type)
                    {
                        case 100000001://Trồng mía
                            {
                                Entity en = new Entity("new_trongmia");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                en["subject"] = qtc["new_name"];
                                en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                en["new_hopdongtrongmia"] = new_hopdongdautumia;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                en["new_hangmucanhtac"] = qtc["new_hangmuccanhtac"];
                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                //en["new_lanbon"] = qtc["new_lanthuchien"];
                                en["new_vutrong"] = new OptionSetValue(new_vutrong);

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                                //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình '{1}'", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_ngaytrongxulygoc"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                //if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    en["new_ngaytrongxulygoc"] = qtc["new_sothoigianthuchien"];
                                en["regardingobjectid"] = target.ToEntityReference();
                                service.Create(en);
                            }
                            break;
                        case 100000002://Bón phân
                            {
                                Entity bp = new Entity("new_bonphan");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                bp["subject"] = qtc["new_name"];
                                bp["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                bp["new_hopdongtrongmia"] = new_hopdongdautumia;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                bp["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                bp["new_lanbon"] = qtc["new_lanthuchien"];

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName,quytrinhcanhtac["new_name"]));
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    bp["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                bp["new_ngaybondukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    bp["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                                bp["regardingobjectid"] = target.ToEntityReference();
                                service.Create(bp);
                            }
                            break;
                        case 100000003://Xử lý cỏ dại
                            {
                                //new_xulycodai
                                Entity en = new Entity("new_xulycodai");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập Tên QT canh tác chi tiết của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                en["subject"] = qtc["new_name"];
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                en["new_hopdongtrongmia"] = new_hopdongdautumia;
                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_lanxuly"] = qtc["new_lanthuchien"];

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_ngayxulydukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                                en["regardingobjectid"] = target.ToEntityReference();
                                service.Create(en);
                            }
                            break;
                        case 100000006://Tưới mía
                            {
                                Entity en = new Entity("new_tuoimia");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}' của quy trình canh tac '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["subject"] = qtc["new_name"];
                                en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                en["new_hopdongtrongmia"] = new_hopdongdautumia;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_solantuoi"] = qtc["new_lanthuchien"];

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                                //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_ngaytuoidukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    en["new_thoigiantuoi"] = qtc["new_sothoigianthuchien"];
                                en["regardingobjectid"] = target.ToEntityReference();
                                service.Create(en);
                            }
                            break;
                        case 100000004: //Xử lý sâu bệnh 
                            {
                                {
                                    Entity en = new Entity("new_xulysaubenh");
                                    if (!qtc.Attributes.Contains("new_name"))
                                        throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}' của quy trình canh tac '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                    en["subject"] = qtc["new_name"];
                                    en["new_chitiethddtmia"] = new EntityReference(target.LogicalName, target.Id);
                                    en["new_hopdongdautumia"] = new_hopdongdautumia;
                                    if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                        throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                    en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                    if (!qtc.Attributes.Contains("new_lanthuchien"))
                                        throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                    en["new_lanxuly"] = qtc["new_lanthuchien"];

                                    //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                                    //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                    if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                        throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                    en["new_ngayxulydukien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                    if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                        en["new_ngayxulythucte"] = qtc["new_sothoigianthuchien"];
                                    en["regardingobjectid"] = target.ToEntityReference();
                                    service.Create(en);
                                }
                            }
                            break;
                        case 100000007:
                        case 100000005:
                        case 100000008:
                        case 100000009:
                        case 100000000:
                        default:
                            //100000007:Khai mương chống úng" Khai mương chống úng
                            //100000007:Bóc lột lá mía || 100000008:khach || 100000009:San lấp mặt bằng
                            //100000000:Cày
                            {
                                Entity nk = new Entity("new_nhatkydongruong");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                nk["subject"] = qtc["new_name"];
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                nk["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                nk["new_hopdongdautumia"] = new_hopdongdautumia;
                                nk["new_thuadatcanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                if (new_khachhang.LogicalName == "contact")
                                    nk["new_khachhang"] = new_khachhang;
                                else if (new_khachhang.LogicalName == "account")
                                    nk["new_khachhangdoanhnghiep"] = new_khachhang;
                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                nk["new_dukienthuchien"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    nk["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                                nk["regardingobjectid"] = target.ToEntityReference();
                                service.Create(nk);
                            }
                            break;
                    }
                }
            }//using
        }
    }
}
