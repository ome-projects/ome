# Deliberately separate from Alfred's canonical admission guard. Only a fresh
# fixture UID, Alfred identity and newly added migration annotation can match.
def retry_policy($name;$uid;$username):
  {apiVersion:"admissionregistration.k8s.io/v1",kind:"ValidatingAdmissionPolicy",
   metadata:{name:$name,labels:{"alfred-e2e.ome.io/run":$name}},
   spec:{failurePolicy:"Fail",matchConstraints:{matchPolicy:"Exact",objectSelector:{},
     namespaceSelector:{matchLabels:{"kubernetes.io/metadata.name":"alfred-e2e"}},
     resourceRules:[{operations:["UPDATE"],apiGroups:["ome.io"],apiVersions:["v1beta1"],
       resources:["inferenceservices"],resourceNames:["single"],scope:"Namespaced"}]},
     matchConditions:[
       {name:"alfred-identity",expression:("request.userInfo.username == "+($username|tojson))},
       {name:"fresh-fixture",expression:("object.metadata.uid == "+($uid|tojson)+" && oldObject.metadata.uid == "+($uid|tojson))},
       {name:"new-migration-request",expression:"has(object.metadata.annotations) && object.metadata.annotations.exists(k, k.startsWith('ome.io/migration-request-v1-') && (!has(oldObject.metadata.annotations) || !(k in oldObject.metadata.annotations)))"}],
     validations:[{expression:"false",message:"semantic-retry publication held"}]}};
def retry_binding($name):
  {apiVersion:"admissionregistration.k8s.io/v1",kind:"ValidatingAdmissionPolicyBinding",
   metadata:{name:$name,labels:{"alfred-e2e.ome.io/run":$name}},spec:{policyName:$name,validationActions:["Deny"]}};
