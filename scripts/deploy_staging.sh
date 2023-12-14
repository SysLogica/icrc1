    dfx deploy icrc1-staging --argument '( record {               
        name = "Windoge98TEST";                         
        symbol = "EXETEST";                           
        decimals = 8;                               
        fee = 1_000_000; 
        max_supply = 1_000_000_000_000_000;
        logo = "data:image/jpeg;base64,...2Q==";                                        
        initial_balances = vec {                                
            record {                                            
                record {
                    owner = principal "3scjg-z33zr-ll3bt-hdsov-kkitm-3tsml-tvgkt-d6jwa-onz35-hkfq5-zae";   
                    subaccount = null;                          
                };                                              
                1_000_000_000_000_000                                     
            }                                                   
        };                                                             
        min_burn_amount = 10_000;
        minting_account = (
            opt record {
                owner=principal "xscoq-ggfay-isg3d-ivuvu-6qpnt-mvlt6-llf4q-ve6q7-3nht5-4ab7q-4qe"; 
                subaccount=null
            }
        );
        advanced_settings = null;
    })' --network ic