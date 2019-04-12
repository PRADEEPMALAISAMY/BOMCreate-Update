package com.rits.cabinet;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import com.sap.me.common.CustomValue;
import com.sap.me.common.ItemType;
import com.sap.me.common.ObjectReference;
import com.sap.me.demand.ShopOrderServiceInterface;
import com.sap.me.frame.SystemBase;
import com.sap.me.frame.domain.BusinessException;
import com.sap.me.integration.frame.workflow.PluginInterface;
import com.sap.me.integration.frame.workflow.ResponseStatus;
import com.sap.me.integration.frame.workflow.WorkflowContext;
import com.sap.me.integration.frame.workflow.WorkflowResponse;
import com.sap.me.integration.util.XMLHandler;
import com.sap.me.productdefinition.BOMComponentConfiguration;
import com.sap.me.productdefinition.BOMConfiguration;
import com.sap.me.productdefinition.BOMConfigurationServiceInterface;
import com.sap.me.productdefinition.BomType;
import com.sap.me.productdefinition.CannotDesignateAsTSMException;
import com.sap.me.productdefinition.DuplicateCompOperRefDesignatorCombinationException;
import com.sap.me.productdefinition.FloorLifeExceedsShelfLifeException;
import com.sap.me.productdefinition.InvalidAssemblyDataTypeException;
import com.sap.me.productdefinition.InvalidBOMComponentsFromTemplateException;
import com.sap.me.productdefinition.ItemConfigurationServiceInterface;
import com.sap.me.productdefinition.ItemFullConfiguration;
import com.sap.me.productdefinition.ItemInputException;
import com.sap.me.productdefinition.KitCannotHaveAlternateException;
import com.sap.me.productdefinition.MaterialNotFoundException;
import com.sap.me.productdefinition.MissingSlotQuantityException;
import com.sap.me.productdefinition.NotUniqueDesignatorsInBomException;
import com.sap.me.productdefinition.PhantomComponentReferenceException;
import com.sap.me.productdefinition.PhantomComponentSequenceNotExistException;
import com.sap.me.productdefinition.ReferenceDesignatorValueAbsentForComponentException;
import com.sap.me.productdefinition.SameComponentAtSameOperationException;
import com.sap.me.security.RunAsServiceLocator;
import com.sap.tc.logging.Category;
import com.sap.tc.logging.Location;
import com.sap.tc.logging.Severity;
import com.sap.tc.logging.SimpleLogger;
import com.visiprise.frame.configuration.ServiceReference;
public class BomCreateAndUpdateMaterial implements PluginInterface  {
	private static final long serialVersionUID = 1L;
	private BOMConfigurationServiceInterface bOMConfigurationService;
	private ShopOrderServiceInterface shopOrderService;
	private ItemConfigurationServiceInterface itemConfigurationService;
	private String site = "";
	private String user = "MESYS";
	private final SystemBase dbBase = SystemBase.createSystemBase("jdbc/jts/wipPool");
	private static final Category category = Category.getCategory(Category.APPLICATIONS, "/ME/Extension/Execution");
	private static final Location loc = Location.getLocation("com.alphadev.hook.BomConfigHook");
	private static final String MESSAGE_ID = "MEExtension:BomConfigHook";
	private static final String SHOP_ORDER = "";// "MII_ASSY_SO_2";
	private ArrayList<String> successBomItemList = new ArrayList<String>();
	private ArrayList<String> failBomItemList = new ArrayList<String>();
	@Override
	public WorkflowResponse processMessage(String plant, WorkflowContext workflowContext, Document inputXml) {
		this.site = plant;
		initServices();
		inputXml.getDocumentElement().normalize();
		NodeList kpnNameNode = inputXml.getElementsByTagName("KPN_NAME");
		String parentComponent = kpnNameNode.item(0).getTextContent();
		String parentComponentRef = getCurrentItemBoOnItem(parentComponent);
		if (parentComponent != null && !"".equalsIgnoreCase(parentComponent)) {
			//readCreateBomAndUpdateToMaterial(BomCreateAndUpdateMaterial.SHOP_ORDER);
			readCreateBomAndUpdateToMaterial(parentComponent);
			if (failBomItemList.size() == 0)
				return successResponse("All Items' Bom Created.", this.successBomItemList);
			else if (successBomItemList.size() == 0) {
				return errorResponse("Bom Couldn't Create.", this.failBomItemList);
			} else
				return partialResponse("Partially Succeed, Please Check Response xml.", this.successBomItemList, this.failBomItemList);
		} else {
			return errorResponse("Material - " + parentComponent + " not present in ME System.", this.failBomItemList);
		}
	}

	public static Element getSoapEnvelope(Document document) {
		Element soapEnvelope = document.getDocumentElement();
		soapEnvelope.setPrefix("SOAP-ENV");
		soapEnvelope.setAttribute("xmlns:SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/");
		soapEnvelope.setAttribute("xmlns:xs", "http://www.w3.org/2001/XMLSchema");
		soapEnvelope.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
		soapEnvelope.setAttribute("xmlns:ns2", "http://sap.com/xi/ME");
		soapEnvelope.setAttribute("xmlns:sch", "http://sap.com/xi/ME/erpcon");
		soapEnvelope.setAttribute("xmlns:me", "http://sap.com/xi/ME");
		soapEnvelope.setAttribute("xmlns:meint", "http://sap.com/xi/MEINT");
		soapEnvelope.setAttribute("xmlns:gdt", "http://sap.com/xi/SAPGlobal/GDT");
		return soapEnvelope;
	}
	
	public WorkflowResponse errorResponse(String errorMessage, ArrayList<String> failBomItemList) {

		WorkflowResponse errorWorkflowResp1 = null;
		DOMImplementation domImplementation;

		Document responseDoc2 = null;
		try {
			domImplementation = XMLHandler.createDOMImplementation();

			responseDoc2 = domImplementation.createDocument("http://sap.com/xi/ME", "Envelope", null);

			Element soapEnvelope = getSoapEnvelope(responseDoc2);

			Element soapBody = responseDoc2.createElement("SOAP-ENV:Body");
			Element customSfcDataValueCreateResponse = responseDoc2.createElementNS("http://sap.com/xi/ME", "ns2:BomCreateAndUpdateMaterialResponse");

			Element messagetag = responseDoc2.createElement("ns2:message");
			messagetag.setTextContent(errorMessage.toString());
			customSfcDataValueCreateResponse.appendChild(messagetag);

			Element failedValuetag = responseDoc2.createElement("ns2:failedSfcValue");

			for (String item : failBomItemList) {
				Element itemTag = responseDoc2.createElement("ns2:item");
				itemTag.setTextContent(item);
				failedValuetag.appendChild(itemTag);
			}

			customSfcDataValueCreateResponse.appendChild(failedValuetag);
			
			Element statustag = responseDoc2.createElement("ns2:statusValue");
			statustag.setTextContent("APP_ERROR");
			customSfcDataValueCreateResponse.appendChild(statustag);

			soapBody.appendChild(customSfcDataValueCreateResponse);
			soapEnvelope.appendChild(soapBody);

			errorWorkflowResp1 = new WorkflowResponse(ResponseStatus.APP_ERROR, XMLHandler.xmlToString(responseDoc2), site, null);

		} catch (ParserConfigurationException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID, "errorResponse() - " + e.getMessage());
		} catch (TransformerException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID, "errorResponse() - " + e.getMessage());
		}
		return errorWorkflowResp1;
	}

	public WorkflowResponse successResponse(String successMessage, ArrayList<String> successBomItemList) {

		WorkflowResponse successWorkflowResp = null;
		DOMImplementation domImplementation;

		Document responseDoc2 = null;
		try {
			domImplementation = XMLHandler.createDOMImplementation();

			responseDoc2 = domImplementation.createDocument("http://sap.com/xi/ME", "Envelope", null);

			Element soapEnvelope = getSoapEnvelope(responseDoc2);

			Element soapBody = responseDoc2.createElement("SOAP-ENV:Body");
			Element customSfcDataValueCreateResponse = responseDoc2.createElementNS("http://sap.com/xi/ME", "ns2:BomCreateAndUpdateMaterialResponse");

			Element messagetag = responseDoc2.createElement("ns2:message");
			messagetag.setTextContent(successMessage.toString());
			customSfcDataValueCreateResponse.appendChild(messagetag);

			Element itemValuetag = responseDoc2.createElement("ns2:successMaterial");

			for (String item : successBomItemList) {
				Element itemTag = responseDoc2.createElement("ns2:item");
				itemTag.setTextContent(item);
				itemValuetag.appendChild(itemTag);
			}
			customSfcDataValueCreateResponse.appendChild(itemValuetag);
			
			Element statustag = responseDoc2.createElement("ns2:statusValue");
			statustag.setTextContent("SUCCESS");
			customSfcDataValueCreateResponse.appendChild(statustag);

			soapBody.appendChild(customSfcDataValueCreateResponse);
			soapEnvelope.appendChild(soapBody);

			successWorkflowResp = new WorkflowResponse(ResponseStatus.PASSED, XMLHandler.xmlToString(responseDoc2), site, null);

		} catch (ParserConfigurationException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID, "successResponse() - " + e.getMessage());
		} catch (TransformerException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID, "successResponse() - " + e.getMessage());
		}
		return successWorkflowResp;
	}
	
	public WorkflowResponse partialResponse(String successMessage, ArrayList<String> successBomItemList,ArrayList<String> failBomItemList ) {

		WorkflowResponse successWorkflowResp = null;
		DOMImplementation domImplementation;

		Document responseDoc2 = null;
		try {
			domImplementation = XMLHandler.createDOMImplementation();

			responseDoc2 = domImplementation.createDocument("http://sap.com/xi/ME", "Envelope", null);

			Element soapEnvelope = getSoapEnvelope(responseDoc2);

			Element soapBody = responseDoc2.createElement("SOAP-ENV:Body");
			Element customSfcDataValueCreateResponse = responseDoc2.createElementNS("http://sap.com/xi/ME", "ns2:BomCreateAndUpdateMaterialResponse");

			Element messagetag = responseDoc2.createElement("ns2:message");
			messagetag.setTextContent(successMessage.toString());
			customSfcDataValueCreateResponse.appendChild(messagetag);

			Element itemValuetag = responseDoc2.createElement("ns2:successMaterial");

			for (String item : successBomItemList) {
				Element itemTag = responseDoc2.createElement("ns2:item");
				itemTag.setTextContent(item);
				itemValuetag.appendChild(itemTag);
			}
			customSfcDataValueCreateResponse.appendChild(itemValuetag);
			
			Element itemfailValuetag = responseDoc2.createElement("ns2:failMaterial");

			for (String itemfail : failBomItemList) {
				Element itemfailTag = responseDoc2.createElement("ns2:item");
				itemfailTag.setTextContent(itemfail);
				itemfailValuetag.appendChild(itemfailTag);
			}
			customSfcDataValueCreateResponse.appendChild(itemfailValuetag);
			
			Element statustag = responseDoc2.createElement("ns2:statusValue");
			statustag.setTextContent("SUCCESS");
			customSfcDataValueCreateResponse.appendChild(statustag);

			soapBody.appendChild(customSfcDataValueCreateResponse);
			soapEnvelope.appendChild(soapBody);

			successWorkflowResp = new WorkflowResponse(ResponseStatus.PASSED, XMLHandler.xmlToString(responseDoc2), site, null);

		} catch (ParserConfigurationException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID, "partialResponse() - " + e.getMessage());
		} catch (TransformerException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID, "partialResponse() - " + e.getMessage());
		}
		return successWorkflowResp;
	}
	

	private void readCreateBomAndUpdateToMaterial(String parentComp) {

		try {
			Connection con = null;
			PreparedStatement preparedStatement = null;
			// Reading Database
			String sql = "SELECT I.Handle AS ITEMBO,PRODUCT,I.ITEM_TYPE,PARTPARENT,PART,PRODUCTQTY,Pdimx,Pdimy,Pdimz,PARTQTY,TRANSFERRED FROM AA_BoMParts_KAT INNER JOIN ITEM I ON(I.ITEM=PART) WHERE Product='"+parentComp+"'";
			con = dbBase.getDBConnection();
			preparedStatement = con.prepareStatement(sql);
			ResultSet itemSet = preparedStatement.executeQuery();
			// LIST TO GET ALL DATA FROM THE DATABASE
			List<BomConfigObject> BomList = new ArrayList<BomConfigObject>();
			if (itemSet != null) {
				while (itemSet.next()) {
					BomConfigObject obj = new BomConfigObject();
					obj.setPart(itemSet.getString("PART"));
					obj.setPartParent(itemSet.getString("PARTPARENT"));
					obj.setPartQty(itemSet.getString("PARTQTY"));
					obj.setProductQty(itemSet.getString("PRODUCTQTY"));
					obj.setPartRef(itemSet.getString("ITEMBO"));
					obj.setTransferred(itemSet.getInt("TRANSFERRED"));
					obj.setType(itemSet.getString("ITEM_TYPE"));
					obj.setLength(itemSet.getInt("Pdimx"));
					obj.setBreadth(itemSet.getInt("Pdimy"));
					obj.setHeight(itemSet.getInt("Pdimz"));
					obj.setProduct(itemSet.getString("Product"));

					if (obj.getTransferred() == 0) {
						BomList.add(obj);
					}
				}
			}
			con.close();
			preparedStatement.close();
			itemSet.close();
			// GETTING PARTPARENTS
			Set<String> partParentsSet = new HashSet<String>();
			for (BomConfigObject bomData : BomList) {
				partParentsSet.add(bomData.getPartParent());
			}
			// BOM CREATION
			if (BomList.size() > 0 && partParentsSet.size() > 0) {
				createBOM(BomList, partParentsSet);
			} else {
				SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
						"BOM Components are already created - ");
			}

		} catch (SQLException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
					"SQLException- readCreateBomAndUpdateToMaterial() - " + e.getMessage());
		}
	}

	private void createBOM(List<BomConfigObject> bomList, Set<String> partParentsSet) {
		try {
			for (String bomPartParents : partParentsSet) {
				List<BomConfigObject> bomParts = new ArrayList<BomConfigObject>();
				for (BomConfigObject bomConfig : bomList) {
					if (bomPartParents.equalsIgnoreCase(bomConfig.getPartParent())) {
						bomParts.add(bomConfig);
					}
				}
				String bomName = "BOM" + "_" + bomPartParents;
				BOMConfiguration request = new BOMConfiguration();
				request.setBom(bomName);
				request.setRevision(getRevisionOrIncremntRevisionOfNewBOM(bomName,this.site));
				request.setBomType(BomType.USERBOM);
				List<BOMComponentConfiguration> bomComponentList = new ArrayList<BOMComponentConfiguration>();
				int seq = 10;
				for (BomConfigObject bomCompConfigParts : bomParts) {
					BOMComponentConfiguration bomComponent = new BOMComponentConfiguration();
					bomComponent.setComponentContext(bomCompConfigParts.getPartRef());
					bomComponent.setSequence(new BigDecimal(seq));
					bomComponent.setQuantity(new BigDecimal(bomCompConfigParts.getPartQty()));
					List<CustomValue> customValueList = new ArrayList<CustomValue>();
					CustomValue customValue1 = new CustomValue();
					customValue1.setName("LENGTH");
					customValue1.setValue(bomCompConfigParts.getLength());
					customValueList.add(customValue1);
					CustomValue customValue2 = new CustomValue();
					customValue2.setName("BREADTH");
					customValue2.setValue(bomCompConfigParts.getBreadth());
					customValueList.add(customValue2);
					CustomValue customValue3 = new CustomValue();
					customValue3.setName("HEIGHT");
					customValue3.setValue(bomCompConfigParts.getHeight());
					customValueList.add(customValue3);
					bomComponent.setCustomData(customValueList);
					bomComponentList.add(bomComponent);
					seq += 10;
					updateTransferred(bomCompConfigParts);
				}
				request.setBomComponentList(bomComponentList);

				BOMConfiguration response = bOMConfigurationService.createBOM(request);
				if (response == null) {

					SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
							"bom Already exist - " );
				}

				if (response != null) {
					String bomRef = response.getBomComponentList().get(0).getBomRef();
					ObjectReference objRef = new ObjectReference();
					objRef.setRef(getCurrentItemBoOnItem(bomPartParents));
					ItemFullConfiguration readItemFullConfig = itemConfigurationService.readItem(objRef);
					ItemFullConfiguration updateItemRequest = new ItemFullConfiguration();
					updateItemRequest.setBomRef(bomRef);
					updateItemRequest.setItem(bomPartParents);
					updateItemRequest.setRevision(readItemFullConfig.getRevision());
					updateItemRequest.setLotSize(readItemFullConfig.getLotSize());
					updateItemRequest.setItemType(ItemType.MANUFACTURED);
					updateItemRequest.setQuantityRestriction(readItemFullConfig.getQuantityRestriction());
					updateItemRequest.setModifiedDateTime(readItemFullConfig.getModifiedDateTime());
					itemConfigurationService.updateItem(updateItemRequest);
				}
			}
		} catch (ReferenceDesignatorValueAbsentForComponentException e) {

			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
					"SQLException- createBOM() - " + e.getMessage());
		} catch (NotUniqueDesignatorsInBomException e) {

			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
					"SQLException- createBOM() - " + e.getMessage());
		} catch (DuplicateCompOperRefDesignatorCombinationException e) {

			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
					"SQLException- createBOM() - " + e.getMessage());
		} catch (SameComponentAtSameOperationException e) {

			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
					"SQLException- createBOM() - " + e.getMessage());
		} catch (InvalidBOMComponentsFromTemplateException e) {

			e.printStackTrace();
		} catch (PhantomComponentSequenceNotExistException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
					"SQLException- createBOM() - " + e.getMessage());
		} catch (PhantomComponentReferenceException e) {

			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
					"SQLException- createBOM() - " + e.getMessage());

		} catch (BusinessException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
					"SQLException- createBOM() - " + e.getMessage());
		}
		
	}

	private String getCurrentItemBoOnItem(String bomPartParents) {
		String itemRef = "";
		try {
			String sql = "SELECT HANDLE FROM ITEM WHERE ITEM = '" + bomPartParents + "'";
			Connection con = null;
			PreparedStatement preparedStatement = null;
			con = dbBase.getDBConnection();
			preparedStatement = con.prepareStatement(sql);
			ResultSet itemSet = preparedStatement.executeQuery();
			itemSet.next();
			itemRef = itemSet.getString("HANDLE");
			con.close();
			preparedStatement.close();
			itemSet.close();
		} catch (SQLException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID, "SQLException- getRef() - " + e.getMessage());
		}
		return itemRef;
	}

	private void updateTransferred(BomConfigObject bomCompConfigParts) {
		try {
			Connection con = null;
			PreparedStatement preparedStatement = null;
			String sql = "UPDATE  AA_BoMParts_KAT SET Transferred=1" + "  where  PRODUCT ='"
					+ bomCompConfigParts.getProduct() + "'AND Part ='" + bomCompConfigParts.getPart()
					+ "' AND PartParent ='" + bomCompConfigParts.getPartParent() + "'";
			con = dbBase.getDBConnection();
			preparedStatement = con.prepareStatement(sql);
			preparedStatement.executeUpdate();
			con.close();
			preparedStatement.close();
		} catch (SQLException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID,
					"SQLException- updateTransferred() - " + e.getMessage());
		}
	}
	
	private String getRevisionOrIncremntRevisionOfNewBOM(String bomName, String plant) {
		String newRevision = "";
		Connection con = null;
		PreparedStatement preparedStatement = null;
		String sql = "";
		try {
			sql = "SELECT REVISION FROM BOM WHERE BOM = '" + bomName + "' and CURRENT_REVISION = 'true' and SITE='" + plant + "'";
			con = dbBase.getDBConnection();
			preparedStatement = con.prepareStatement(sql);
			ResultSet itemSet = preparedStatement.executeQuery();

			if (itemSet.next()) {
				String revision = itemSet.getString("REVISION");
				int i;
				String[] strArray = revision.split("-");
				if (strArray.length == 1)
					newRevision = strArray[0] + "-1";
				else {
					i = Integer.parseInt(strArray[1]) + 1;
					newRevision = strArray[0] + "-" + i;
				}
			} else
				newRevision = "A";
		} catch (SQLException e) {
			SimpleLogger.log(Severity.DEBUG, category, loc, MESSAGE_ID, "getRevisionOrIncremntRevisionOfNewBOM() - " + e.getMessage());
		}
		return newRevision;
	}
		
	

	private void initServices() {
		ServiceReference shopOrderServiceRef = new ServiceReference("com.sap.me.demand", "ShopOrderService");
		this.shopOrderService = RunAsServiceLocator.getService(shopOrderServiceRef, ShopOrderServiceInterface.class,
				user, site, null);
		ServiceReference bOMConfigurationServiceRef = new ServiceReference("com.sap.me.productdefinition",
				"BOMConfigurationService");
		this.bOMConfigurationService = RunAsServiceLocator.getService(bOMConfigurationServiceRef,
				BOMConfigurationServiceInterface.class, user, site, null);
		ServiceReference itemConfigurationServiceRef = new ServiceReference("com.sap.me.productdefinition",
				"ItemConfigurationService");
		this.itemConfigurationService = RunAsServiceLocator.getService(itemConfigurationServiceRef,
				ItemConfigurationServiceInterface.class, user, site, null);
	}
}
