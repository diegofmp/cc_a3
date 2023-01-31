import React, { useState } from "react";

const App = () => {
  const [data, setData] = useState(null);

  const fetchData = async () => {
    const api_url  = "http://180.20.128.4:80/test_pipeline"
    const response = await fetch(api_url);
    console.log("ResponsE: ", response)
    
    
    const json = await response.json();
    setData(json);
  };

  return (
    <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh" }}>
      <div style={{ textAlign: "center", display: "inlineBlock"}}>
        <div>
        <button onClick={fetchData}>Run test</button>
        </div>
        {data && (
          <div><p style={{ marginTop: 20 }}>
            {JSON.stringify(data, null, 2)}
          </p></div>
        )}
      </div>
      
    </div>
  );
};

export default App;
