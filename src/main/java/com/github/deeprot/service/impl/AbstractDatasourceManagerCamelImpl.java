package com.github.deeprot.service.impl;

import java.util.List;

import org.apache.camel.CamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import com.github.deeprot.model.DataSet;
import com.github.deeprot.model.DataSource;
import com.github.deeprot.service.CrmDatasourceManager;
/**
 * Abstract implementation of {@link com.hrboss.cxoboss.datasource.crm.CrmDatasourceManager}
 * 
 * @author bruce.nguyen
 *
 */
public abstract class AbstractDatasourceManagerCamelImpl implements CrmDatasourceManager {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractDatasourceManagerCamelImpl.class);
	
	@Autowired
	protected CamelContext camelContext;
	
    //@Autowired
    //protected DataSourceDao<DataSource> dataSourceDao;
    
    //@Autowired
    //protected DataSetDao datasetDao;	

	@Override
	public DataSource save(DataSource dataSource) throws Exception {
		Assert.notNull(dataSource, "[dataSource] must not be NULL");
		try {
            //dataSourceDao.save(dataSource);
            List<DataSet> tablesMetadataList = buildObjectsMetadata(dataSource, null);
            for (DataSet dataset : tablesMetadataList) {
            	dataset.setDataSourceId(dataSource.getId());
            	dataset.setOwnerId(dataSource.getOwner());
            	dataset.setRoles(dataSource.getRoles());
            	//datasetDao.save(datasetDao.getCollection(), dataset);
            	dataset.setOriginalDataSetId(dataset.getId());
            	//datasetDao.update(datasetDao.getCollection(), dataset);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
		return dataSource;
	}

}
