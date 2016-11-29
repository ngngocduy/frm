using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Messaging;

namespace PDNTamUng_ETLTranSaction
{
    public class PDNTamUng_ETLTranSaction : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyet
            {
                Entity fullEntity = (Entity)context.PostEntityImages["PostImg"];
                Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));
                Entity HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)fullEntity["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                Entity KH = null;

                if (fullEntity.Contains("new_khachhang"))
                    KH = service.Retrieve("contact", ((EntityReference)fullEntity["new_khachhang"]).Id,
                        new ColumnSet(new string[] { "new_makhachhang", "new_socmnd", "new_phuongthucthanhtoan" }));
                else
                    KH = service.Retrieve("account", ((EntityReference)fullEntity["new_khachhangdoanhnghiep"]).Id,
                        new ColumnSet(new string[] { "new_makhachhang", "new_masothue", "new_phuongthucthanhtoan" }));

                if (fullEntity.Contains("new_sotienung") && ((Money)fullEntity["new_sotienung"]).Value > 0)
                {
                    #region begin

                    Entity etl_ND = new Entity("new_etltransaction");
                    etl_ND["new_vouchernumber"] = "DTND";
                    etl_ND["new_transactiontype"] = "1.2.5.a";
                    etl_ND["new_season"] = Vudautu["new_mavudautu"].ToString();
                    etl_ND["new_sochungtu"] = fullEntity["new_masophieutamung"].ToString();
                    etl_ND["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                    etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                        :
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                        );
                    etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                    etl_ND["new_suppliersite"] = "TAY NINH";
                    etl_ND["new_invoicedate"] = fullEntity["new_ngayduyet"];
                    etl_ND["new_descriptionheader"] = "Tạm ứng tiền mặt cho nông dân";
                    etl_ND["new_terms"] = "Tra Ngay";
                    etl_ND["new_taxtype"] = "";
                    etl_ND["new_invoiceamount"] = fullEntity["new_sotienung"];
                    etl_ND["new_gldate"] = fullEntity["new_ngayduyet"];
                    etl_ND["new_invoicetype"] = "PREPAY";
                    etl_ND["new_paymenttype"] = "TM";

                    if (fullEntity.Contains("new_khachhang"))
                        etl_ND["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        etl_ND["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    Guid etl_NDID = service.Create(etl_ND);
                    Send(etl_ND);

                    #endregion

                    if (fullEntity.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)fullEntity["new_phuongthucthanhtoan"]).Value == 100000001)
                    {
                        #region Pay nếu là chuyển khoản
                        Entity paytamung = new Entity("new_applytransaction");
                        //apply_PGNPhanbon["new_documentsequence"] = value++;
                        paytamung["new_suppliersitecode"] = "Tây Ninh";


                        List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                            new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                            KH.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", KH.Id);

                        Entity taikhoanchinh = null;

                        foreach (Entity en in taikhoannganhang)
                        {
                            if ((bool)en["new_giaodichchinh"] == true)
                                taikhoanchinh = en;
                        }

                        paytamung["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                        paytamung["new_paymentamount"] = fullEntity["new_sotienung"];
                        paytamung["new_paymentdate"] = fullEntity["new_ngayduyet"];
                        paytamung["new_paymentdocumentname"] = "CANTRU_03";
                        paytamung["new_vouchernumber"] = "BN";
                        paytamung["new_cashflow"] = "00.00";
                        paytamung["new_paymentnum"] = 1;
                        paytamung["new_documentnum"] = fullEntity["new_masophieutamung"].ToString();

                        if (fullEntity.Contains("new_khachhang"))
                            paytamung["new_khachhang"] = fullEntity["new_khachhang"];
                        else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                            paytamung["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                        service.Create(paytamung);
                        Send(paytamung);
                        #endregion
                    }
                }
            }
        }

        public static string Serialize(object obj)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamReader reader = new StreamReader(memoryStream))
            {
                DataContractSerializer serializer = new DataContractSerializer(obj.GetType());
                serializer.WriteObject(memoryStream, obj);
                memoryStream.Position = 0;
                return reader.ReadToEnd();
            }
        }

        public static object Deserialize(string xml, Type toType)
        {
            using (Stream stream = new MemoryStream())
            {
                byte[] data = System.Text.Encoding.UTF8.GetBytes(xml);
                stream.Write(data, 0, data.Length);
                stream.Position = 0;
                DataContractSerializer deserializer = new DataContractSerializer(toType);
                return deserializer.ReadObject(stream);
            }
        }

        private void Send(Entity tmp)
        {
            MessageQueue mq;

            if (MessageQueue.Exists(@".\Private$\DynamicCRM2Oracle"))
                mq = new MessageQueue(@".\Private$\DynamicCRM2Oracle");
            else
                mq = MessageQueue.Create(@".\Private$\DynamicCRM2Oracle");

            Message m = new Message();
            m.Body = Serialize(tmp);
            m.Label = "cust";
            mq.Send(m);
        }

        private List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
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
