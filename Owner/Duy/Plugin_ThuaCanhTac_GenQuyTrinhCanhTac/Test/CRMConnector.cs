using Microsoft.Xrm.Sdk.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.ServiceModel.Description;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    public class CRMConnector
    {
        private OrganizationServiceProxy _service;
        public OrganizationServiceProxy Service { get { return _service; } }
        public void ConnectToCrm()
        {
            try
            {
                string port = GetConfig("port");
                if (port == "80")
                    port = "";
                else
                    port = ":" + port;
                Uri crmUrl = new Uri(string.Format("{0}://{1}{2}/{3}/{4}"
                    , GetConfig("protocol")
                    , GetConfig("server")
                    , port
                    , GetConfig("org")
                    , GetConfig("servicePath")));
                ClientCredentials credential = new ClientCredentials();
                credential.UserName.UserName = GetConfig("userName");
                credential.UserName.Password = GetConfig("password");
                _service = new OrganizationServiceProxy(crmUrl, null, credential, null);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private string GetConfig(string key)
        {
            return ConfigurationManager.AppSettings[key];
        }
    }
}
