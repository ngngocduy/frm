﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Activities;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;


namespace Plugin_ThemCSDTvaoCTNTThuedat
{
    public class Plugin_ThemCSDTvaoCTNTThuedat : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            traceService.Trace(string.Format("Context Depth {0}", context.Depth));
            if (context.Depth > 1)
                return;

            if (context.InputParameters.Contains("Target") && (context.InputParameters["Target"] is Entity))
            {
                Entity target = (Entity)context.InputParameters["Target"];
                Guid targetId = target.Id;

                if (target.LogicalName == "new_chitietnghiemthuthuedat")
                {
                    if (context.MessageName.ToUpper() == "CREATE" || context.MessageName.ToUpper() == "UPDATE")
                    {
                        Entity ChiTietNTTD = service.Retrieve("new_chitietnghiemthuthuedat", targetId, new ColumnSet(true));
                        DateTime ngaytao = ChiTietNTTD.GetAttributeValue<DateTime>("createdon");

                        EntityReference ThuadatRef = ChiTietNTTD.GetAttributeValue<EntityReference>("new_thuadat");
                        Entity thuadatObj = service.Retrieve("new_thuadat", ThuadatRef.Id, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy", "new_diachi", "new_dientich" }));

                        EntityReference NTTDref = ChiTietNTTD.GetAttributeValue<EntityReference>("new_nghiemthuthuedat");
                        Entity NTTD = service.Retrieve("new_nghiemthuthuedat", NTTDref.Id,
                            new ColumnSet(new string[] { "new_hopdongdaututhuedat", "new_datthue",
                                "new_lannghiemthu_global", "new_khachhang", "new_khachhangdoanhnghiep" }));

                        EntityReference HDDTThuedatRef = NTTD.GetAttributeValue<EntityReference>("new_hopdongdaututhuedat");
                        Entity HDDTThuedat = service.Retrieve("new_hopdongthuedat", HDDTThuedatRef.Id, new ColumnSet(new string[] { "new_vudautu", "new_khachhang", "new_khachhangdoanhnghiep" }));

                        EntityReference ChiTietHDThueDatRef = NTTD.GetAttributeValue<EntityReference>("new_datthue");
                        Entity ChiTietHDThueDat = service.Retrieve("new_datthue", ChiTietHDThueDatRef.Id,
                            new ColumnSet(new string[] { "new_hopdongthuedat", "new_benchothuedatkh", "new_benchothuedatkhdn" }));

                        Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)HDDTThuedat["new_vudautu"]).Id,
                            new ColumnSet(new string[] { "new_name" }));
                        Entity ctHDTDTD = new Entity("new_chitiethdthuedat_thuadat");

                        Entity en = new Entity("new_chitietnghiemthuthuedat");
                        en.Id = targetId;

                        EntityCollection listCTHDTDthuadat = FindctHDTDTD(service, ChiTietHDThueDat);
                        if (listCTHDTDthuadat != null && listCTHDTDthuadat.Entities.Count > 0)
                        {
                            string thuadatId = thuadatObj.Id.ToString();

                            foreach (Entity cthdtdtd in listCTHDTDthuadat.Entities)
                            {
                                EntityReference tdref = cthdtdtd.GetAttributeValue<EntityReference>("new_thuadat");
                                Entity td = service.Retrieve("new_thuadat", tdref.Id, new ColumnSet(new string[] { "new_nhomdat" }));
                                string tdId = td.Id.ToString();
                                if (thuadatId == tdId)
                                {
                                    ctHDTDTD = service.Retrieve("new_chitiethdthuedat_thuadat", cthdtdtd.Id, new ColumnSet(new string[] { "createdon", "new_chitiethdthuedat", "new_thuadat", "new_dinhmuc", "new_chinhsachdautu", "new_sonamthuedat", "new_dientichthucthue" }));
                                    break;
                                }
                            }
                        } // if(listCTHDTDthuadat != null && listCTHDTDthuadat.Entities.Count > 0)
                        if (ctHDTDTD != null)
                        {
                            Entity mCSDT = new Entity();
                            EntityCollection CSDTcol = FindCSDTTD(service, ngaytao, Vudautu);
                            traceService.Trace(CSDTcol.Entities.Count.ToString());
                            if (CSDTcol != null && CSDTcol.Entities.Count > 0)
                            {
                                foreach (Entity a in CSDTcol.Entities)
                                {
                                    traceService.Trace(a["new_name"].ToString());
                                    if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                    {
                                        if (ChiTietNTTD.Attributes.Contains("new_nhomdat"))
                                        {
                                            if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)ChiTietNTTD["new_nhomdat"]).Value.ToString()) == -1)
                                                continue;
                                        }
                                    }

                                    traceService.Trace("Pass nhom dat");

                                    if (a.Contains("new_sonamthue"))  // So nam thue
                                    {
                                        if (ChiTietNTTD.Contains("new_thoigianthue"))
                                        {
                                            int sonamthueCSDT = (int)a["new_sonamthue"];
                                            int sonamthueCTNTTD = (int)ChiTietNTTD["new_thoigianthue"];
                                            traceService.Trace(sonamthueCSDT.ToString() + "-" + sonamthueCTNTTD.ToString());
                                            if (sonamthueCSDT != sonamthueCTNTTD)
                                                continue;
                                        }
                                        else
                                            continue;
                                    }

                                    traceService.Trace("Pass so nam thue dat");

                                    // NHom khach hang
                                    bool co = false;

                                    if (NTTD.Contains("new_khachhang"))
                                    {
                                        Guid khId = ((EntityReference)NTTD["new_khachhang"]).Id;
                                        Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                        EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                        if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                        {
                                            EntityReference nhomkhEntityRef = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang");
                                            Guid nhomkhId = nhomkhEntityRef.Id;
                                            Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                            if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                            {
                                                foreach (Entity nhomKH in dsNhomKH.Entities)
                                                {
                                                    if (nhomKHHDCT.Id == nhomKH.Id)
                                                    {
                                                        co = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                co = true;
                                            }
                                        }
                                        else   //neu khong co NHomKH trong CTHD
                                        {
                                            if (dsNhomKH == null || dsNhomKH.Entities.Count == 0)
                                            {
                                                co = true;
                                            }
                                        }
                                    }

                                    if (NTTD.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        Guid khId = ((EntityReference)NTTD["new_khachhangdoanhnghiep"]).Id;
                                        Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                        EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                        if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                        {
                                            Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                            Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                            if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                            {
                                                foreach (Entity nhomKH in dsNhomKH.Entities)
                                                {
                                                    if (nhomKHHDCT.Id == nhomKH.Id)
                                                    {
                                                        co = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                co = true;
                                            }
                                        }
                                        else   //neu khong co NHomKH trong CTHD
                                        {
                                            if (dsNhomKH == null || dsNhomKH.Entities.Count == 0)
                                            {
                                                co = true;
                                            }
                                        }
                                    }

                                    if (co == false)
                                        continue;
                                    traceService.Trace("Pass DK nhom KH");

                                    //Vung dia ly
                                    EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                                    traceService.Trace("so vung DL " + dsVungDL.Entities.Count());

                                    if (dsVungDL.Entities.Count > 0)
                                    {
                                        co = false;

                                        List<Guid> dsvung = new List<Guid>();
                                        foreach (Entity n in dsVungDL.Entities)
                                            dsvung.Add(n.Id);

                                        if (thuadatObj.Attributes.Contains("new_diachi"))
                                        {
                                            traceService.Trace("thu dat co dia chi ");
                                            Entity diachi = service.Retrieve("new_diachi", ((EntityReference)thuadatObj["new_diachi"]).Id, new ColumnSet(new string[] { "new_path" }));
                                            traceService.Trace("lay duoc DC ");
                                            QueryExpression qe = new QueryExpression("new_vungdialy_hanhchinh");
                                            qe.ColumnSet = new ColumnSet(new string[] { "new_vungdialy_hanhchinhid", "new_vungdialy", "new_path" });
                                            qe.Criteria.AddCondition(new ConditionExpression("new_vungdialy", ConditionOperator.In, dsvung.ToArray()));

                                            traceService.Trace("duong dan " + service.RetrieveMultiple(qe).Entities.Count());
                                            traceService.Trace("diachi : " + diachi["new_path"].ToString());

                                            foreach (Entity n in service.RetrieveMultiple(qe).Entities)
                                            {
                                                traceService.Trace("vung dia ly hanh chinh :" + n["new_path"].ToString());

                                                //if (diachi["new_path"].ToString().Contains(n["new_path"].ToString()))
                                                //{
                                                //    co = true;
                                                //    break;
                                                //}

                                                if (diachi["new_path"].ToString().Contains(n["new_path"].ToString()))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        if (co == false)
                                            continue;

                                    }

                                    traceService.Trace("Pass vung Dia Ly");

                                    // Nhom cu ly
                                    co = false;

                                    EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", a.Id);
                                    if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                    {
                                        EntityReference nhomclEntityRef = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy");
                                        Guid nhomclId = nhomclEntityRef.Id;
                                        Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId, new ColumnSet(new string[] { "new_name" }));

                                        if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                        {
                                            foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                            {
                                                if (nhomCL.Id == nhomCL1.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            co = true;
                                        }
                                    }
                                    else   //neu khong co NHomCL trong CTHD
                                    {

                                        if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                    if (co == false)
                                        continue;

                                    traceService.Trace("Pass Nhom cu ly");

                                    // NHom nang suat
                                    co = false;

                                    if (NTTD.Attributes.Contains("new_khachhang"))
                                    {
                                        EntityReference khEntityRef = NTTD.GetAttributeValue<EntityReference>("new_khachhang");
                                        Guid khId = khEntityRef.Id;
                                        Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));

                                        EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                        traceService.Trace("dsNhomNS " + dsNhomNS.Entities.Count());

                                        if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                        {
                                            decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                            if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                            {
                                                foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                {
                                                    if (mhkn1.Attributes.Contains("new_nangsuattu") && mhkn1.Attributes.Contains("new_nangsuatden"))
                                                    {
                                                        decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                        decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                        if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                        {
                                                            co = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                co = true;
                                            }
                                        }
                                        else
                                        {
                                            if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                            {
                                                co = true;
                                            }
                                        }
                                    }
                                    if (NTTD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        Guid khId = NTTD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                        Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                        EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                        if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                        {
                                            decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                            if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                            {
                                                foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                {
                                                    if (mhkn1.Attributes.Contains("new_nangsuattu") && mhkn1.Attributes.Contains("new_nangsuatden"))
                                                    {
                                                        decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                        decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                        if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                        {
                                                            co = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                co = true;
                                            }
                                        }
                                        else
                                        {
                                            if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                            {
                                                co = true;
                                            }
                                        }
                                    }

                                    if (co == false)
                                        continue;

                                    traceService.Trace("Pass Nhom nang suat");
                                    traceService.Trace("co " + co.ToString());

                                    mCSDT = a;
                                    break;
                                }
                            }
                            
                            if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            {

                                traceService.Trace("Tim duoc CSDT");

                                //check so lan nghiem thu
                                QueryExpression q1 = new QueryExpression("new_dinhmucdautu");
                                q1.ColumnSet = new ColumnSet(true);
                                q1.Criteria.AddCondition(new ConditionExpression("new_chinhsachdautu", ConditionOperator.Equal, mCSDT.Id));
                                EntityCollection entc = service.RetrieveMultiple(q1);
                                int solangiaingan = entc.Entities.Count;
                                int solannt = NTTD.Contains("new_lannghiemthu_global")
                                    ? ((OptionSetValue)NTTD["new_lannghiemthu_global"]).Value
                                    : 0;
                                solannt = solannt - 100000000 + 1;
                                if (solannt > solangiaingan)
                                {
                                    throw new Exception("Số lần nghiệm thu vượt quá số lần giải ngân");
                                }

                                // ------Gan vao Chi tiet PDN giải ngân 
                                EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                en["new_chinhsachdautu"] = csdtRef;

                                Guid csdtKQ = mCSDT.Id;
                                Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ,
                                    new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_dinhmucdautukhonghoanlai" }));

                                decimal lanNT = NTTD.GetAttributeValue<OptionSetValue>("new_lannghiemthu_global").Value + 2;
                                traceService.Trace("lanNT " + lanNT);

                                decimal tyle = 0;
                                // Tim dinh muc dau tu
                                EntityCollection DinhmucDTTMcol = FindDaututienmat(service, csdtKQEntity);

                                if (DinhmucDTTMcol != null && DinhmucDTTMcol.Entities.Count > 0)
                                {
                                    foreach (Entity dmdttm in DinhmucDTTMcol.Entities)
                                    {
                                        decimal ycNT = dmdttm.GetAttributeValue<OptionSetValue>("new_yeucau").Value;
                                        traceService.Trace("ycNT " + ycNT);

                                        if (ycNT == lanNT)
                                        {
                                            if (dmdttm.Contains("new_tyleyc"))
                                                tyle = (dmdttm.Contains("new_tyleyc") ? (decimal)dmdttm["new_tyleyc"] : 0);
                                            else
                                                tyle = (dmdttm.Contains("new_phantramtilegiaingan") ? (decimal)dmdttm["new_phantramtilegiaingan"] : 0);
                                            break;
                                        }
                                    }
                                }

                                traceService.Trace("tyle " + tyle);
                                solannt = NTTD.Contains("new_lannghiemthu_global")
                                   ? ((OptionSetValue)NTTD["new_lannghiemthu_global"]).Value
                                   : 0;
                                decimal tienHL = 0;

                                // --------  Gan Dinh muc
                                QueryExpression q = new QueryExpression("new_dinhmucdautu");
                                q.ColumnSet = new ColumnSet(true);
                                q.Criteria.AddCondition(new ConditionExpression("new_chinhsachdautu", ConditionOperator.Equal, mCSDT.Id));

                                foreach (Entity a in service.RetrieveMultiple(q).Entities)
                                {
                                    if (!a.Contains("new_yeucau"))
                                        continue;

                                    if (((OptionSetValue)a["new_yeucau"]).Value == solannt + 2)
                                    {
                                        tienHL = ((Money)a["new_sotien"]).Value;
                                    }
                                }
                                // -------- End  Gan Dinh muc

                                decimal sotiendautu = (ChiTietNTTD.Contains("new_thoigianthue")
                                    ? (int)ChiTietNTTD["new_thoigianthue"] : 0) * tienHL * (ChiTietNTTD.Contains("new_dientichnghiemthu")
                                    ? (decimal)ChiTietNTTD["new_dientichnghiemthu"] : 0);

                                traceService.Trace("So tien dau tu " + sotiendautu);

                                //en["new_tyle"] = tyle;
                                en["new_dinhmuc"] = new Money(tienHL);
                                //en["new_sotiendautu"] = new Money(sotiendautu);

                                service.Update(en);
                            }
                            else
                                throw new InvalidPluginExecutionException("Không có Chính sách Đầu tư thuê đất phù hợp");
                        } // if(ctHDTDTD != null)
                    }
                }
            }
        }
        EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection collRecords = service.RetrieveMultiple(query);

            return collRecords;
        }

        public static EntityCollection FindctHDTDTD(IOrganizationService crmservices, Entity ctHDTD)
        {
            string fetchXml =
                   @"<fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name = 'new_chitiethdthuedat_thuadat' >
                        <attribute name='new_thuadat' />
                        <attribute name = 'new_sotiendaututhucte' />
                        <attribute name='new_sotiendautu' />
                        <attribute name = 'new_sonamthuedat' />
                        <attribute name='new_hientrangcaytrong' />
                        <attribute name = 'new_dientichthuehd' />
                        <attribute name='new_dientichthucthue' />
                        <attribute name = 'new_chitiethdthuedat_thuadatid' />
                        <order attribute='new_thuadat' descending='false' />
                        <filter type = 'and' >
                          <condition attribute='new_chitiethdthuedat' operator='eq' uitype='new_datthue' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, ctHDTD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTTD(IOrganizationService crmservices, DateTime ngaytao, Entity Vudt)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                     <entity name='new_chinhsachdautu'>
                               <attribute name='new_name' />
                               <attribute name='new_vudautu' />
                               <attribute name='new_ngayapdung' />
                               <attribute name='new_mucdichdautu' />
                               <attribute name='new_loaihopdong' />
                               <attribute name='new_dinhmucdautukhonghoanlai' />
                               <attribute name='new_dinhmucdautuhoanlai' />
                               <attribute name='new_sonamthue' />
                               <attribute name='new_chinhsachdautuid' />
                           <order attribute='new_ngayapdung' descending='true' />
                           <filter type='and'>
                               <condition attribute='new_loaihopdong' operator='eq' value='100000001' />
                               <condition attribute='new_mucdichdautu' operator='eq' value='100000005' />
<condition attribute='statecode' operator='eq' value='0' />
                               <condition attribute='new_ngayapdung' operator='le' value='{0}' />
                               <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                        
                           </filter>
                       </entity>
                     </fetch>";

            fetchXml = string.Format(fetchXml, ngaytao, Vudt.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindDaututienmat(IOrganizationService crmservices, Entity CSDT)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_dinhmucdautu'>
                    <attribute name='new_name' />
                    <attribute name='new_sotien' />
                    <attribute name='new_langiaingan' />
                    <attribute name='new_phantramtilegiaingan' />
                    <attribute name='new_dinhmucdautuid' />
                    <attribute name='new_yeucau' />
                    <attribute name='new_tyleyc' />
                    <attribute name='new_sotienyc' />
                    <attribute name='new_phanbontoithieuyc' />
                    <attribute name='new_phanbontoithieubt' />
                    <order attribute='new_langiaingan' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, CSDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        Entity GetThuadatcanhtacfromThuadat(Entity Thuadat, Entity hopdong)
        {
            QueryExpression q = new QueryExpression("new_thuadatcanhtac");
            q.ColumnSet = new ColumnSet(new string[] { "new_yeucaudacbiet" });
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hopdong.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, Thuadat.Id));
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));

            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.FirstOrDefault();
        }
    }
}
