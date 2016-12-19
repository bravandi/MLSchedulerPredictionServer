package edu.purdue.mlscheduler;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
	
public class BackendWeights_VolumeRequest{
	
	public BackendWeights_VolumeRequest(BigInteger volume_request_id) throws SQLException, Exception{
		this.retrieve(volume_request_id);
	}
	
	public class VolumeRequest{
		public BigInteger id;
		public BigInteger workload_id;
		public int capacity;
		public int type;
		public int read_iops;
		public int write_iops;
		public int create_clock;
	}
	
	public class BackendWeight{
		// not used
		public BigInteger backend_id;
		// not used
		public String cinder_id;
		public int live_volume_count_during_clock;
		public int requested_read_iops_total;
		public int requested_write_iops_total;	
	}
	
	public BackendWeight empty_backend_weight_instance(){
		BackendWeight result = new BackendWeight();
		
//		result.backend_id
		result.requested_read_iops_total = 0;
		result.requested_write_iops_total=0;
		result.live_volume_count_during_clock=0;
		
		return result;
	}
	
	public VolumeRequest volume_request;
	
	public Map<String, BackendWeight> backend_weight_map;
	
	public void retrieve(BigInteger volume_request_id) throws SQLException, Exception{
		
		Connection connection = Classification.getConnection();

		this.backend_weight_map = new HashMap<>();
		this.volume_request = new VolumeRequest();
		
		// ex_ID, size
		try (CallableStatement cStmt = connection.prepareCall("{call get_backends_weights(?, ?)}")) {

			cStmt.setBigDecimal(1, BigDecimal.valueOf(0)); // exp_ID
			cStmt.setBigDecimal(2, new BigDecimal(volume_request_id)); // volume_request_id

			cStmt.execute();

			// ResultSet rs = null;

			boolean has_result_set = true;

			boolean is_volume_request_resulset = true;

			while (has_result_set) {

				try (ResultSet rs = cStmt.getResultSet()) {
					
					if (rs == null)

						break;
					
					rs.next();
					
					if(is_volume_request_resulset){
						this.volume_request.id = rs.getBigDecimal("id").toBigInteger();
						this.volume_request.workload_id = rs.getBigDecimal("workload_id").toBigInteger();
						this.volume_request.capacity = rs.getInt("capacity");
						this.volume_request.type = rs.getInt("type");
						this.volume_request.read_iops = rs.getInt("read_iops");
						this.volume_request.write_iops = rs.getInt("write_iops");
						this.volume_request.create_clock = rs.getInt("create_clock");
						
						is_volume_request_resulset = false;
					}
					else{
						BackendWeight backend_weight = new BackendWeight();
						
						backend_weight.backend_id = rs.getBigDecimal("backend_id").toBigInteger();
						backend_weight.cinder_id = rs.getString("cinder_id");
						backend_weight.live_volume_count_during_clock = rs.getInt("live_volume_count_during_clock");
						backend_weight.requested_read_iops_total = rs.getInt("requested_read_iops_total");
						backend_weight.requested_write_iops_total = rs.getInt("requested_write_iops_total");
						
						this.backend_weight_map.put(backend_weight.cinder_id, backend_weight);
					}
				
				}
				
				has_result_set = !((cStmt.getMoreResults() == false) && //
						(cStmt.getUpdateCount() == -1));
			}
		}
	}
}