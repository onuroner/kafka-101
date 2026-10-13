# Gün 1: Temel Bilgiler ve Terimler

## Event Streaming Kavramı

Event streaming kavramı, tek bir olaydan daha çok, bir süreci ifade etmektedir. Bu süreç şu aşamalardan meydana gelmektedir:

* Verilerin veritabanları, sensörler, mobil cihazlar, bulut hizmetleri ve yazılım uygulamaları gibi kaynaklardan olay akışları halinde gerçek zamanlı olarak yakalanması
* Bu olay akışlarının daha sonra erişilmek üzere kalıcı olarak depolanması
* Söz konusu akışlar üzerinde hem gerçek zamanlı hem de geriye dönük olarak işlemler yapılması, bunların işlenmesi ve bunlara yanıt verilmesi
* Gerektiğinde bu akışların farklı hedef teknolojilere yönlendirilmesi

Olay verileri, neyin, ne zaman ve kimlerin o olayda ilgili olduğunu açıklar. Event streaming, verilerin sürekli akışını ve yorumlanmasını sağlayarak doğru bilginin doğru yerde, doğru zamanda olmasını garanti eder.

## Kullanım Senaryoları

Event streaming için birçok kullanım alanı mevcuttur. Gerçek dünyadaki işleyiş biçimine daha çok benzediği için, neredeyse her iş süreci, batch processing'e kıyasla event streaming ile daha iyi temsil edilebilir. Tipik kullanm senaryoları şu şekilde sıralanabilir:

* Anlık lokasyon verisi
* Dolandırıcılık tespiti
* Müşteri/kullanıcı etkinliği
* Log monitoring
* Pazarlama, satış ve iş analitiği

Ve daha fazlası...

## Kafka'nın İşleyişi

Kafka, olay akışına yönelik kullanım senaryolarınızı uçtan uca, tek ve kendini kanıtlamış bir çözümle uygulayabilmeniz için üç temel yeteneği bir araya getirir:

* Olay akışlarını publish etmek ve bunlara subscribe olmak **(pub/sub)**
* Olay akışlarını istediğiniz süre boyunca kalıcı ve güvenilir bir şekilde saklamak **(store)**
* Olay akışlarını meydana geldikleri anda veya geriye dönük olarak işlemek **(processing event streams)**

Bir dosya sistemi veya veritabanı işlem günlüğü (commit log), tüm işlemlerin kalıcı bir kaydını tutmak üzere tasarlanmıştır; böylece bu işlemler yeniden oynatılarak sistemin durumu tutarlı bir şekilde oluşturulabilir. Benzer şekilde, Kafka'daki veriler de kalıcı ve sıralı bir biçimde saklanır ve deterministik olarak okunabilir. Ayrıca veriler sistem genelinde dağıtılarak, hem arızalara karşı ek koruma sağlanabilir hem de performans ölçeklendirme açısından önemli fırsatlar elde edilebilir.

## Terminoloji

### Messages

Kafka'daki veri birimine mesaj adı verilir. Bunu bir veritabanı sistemindeki bir tablodaki bir satır olarak düşünebilirsiniz. Kafka açısından bir messaj, bir byte dizisini ifade eder. Bir mesaj, **"key"** olarak adlandırılan isteğe bağlı bir metadata parçasına sahip olabilir.Key'ler, mesajların partition'lara daha kontrollü bir şekilde yazılması gerektiğinde kullanılır. Aynı key değerine sahip mesajların her zaman aynı partition'a yazılması sağlanabilir.

Verimlilik sağlamak amacıyla, mesajlar Kafka'ya batch'ler halinde yazılır. Bir **batch**, tamamı **aynı topic** ve **partition'a** gönderilen mesajlar kümesinden ibarettir. Her bir mesaj için ağ üzerinden ayrı bir gidiş-dönüş işlemi yapılması aşırı ek yüke (overhead) yol açar; mesajların bir grup halinde toplanması ise bu yükü azaltır.

### Topic ve Partition

Kafka'daki mesajlar **topic'ler** altında sınıflandırılır. Topic kavramını, bir veritabanı tablosu veya dosya sistemindeki bir klasör olarak düşünebilirsiniz. Kafka'daki topic'ler her zaman multi-producer ve multi-subscriber yapısına sahiptir; bir topic'e mesaj yazan sıfır, bir veya birçok producer olabileceği gibi, bu mesajlara abone olan sıfır, bir veya birçok consumer de bulunabilir. Geleneksel mesajlaşma sistemlerinin aksine, bir topic'teki mesajlar consume edildikten sonra silinmez; dolayısıyla ihtiyaç duyulduğu sıklıkta okunabilirler . Bunun yerine, mesajların Kafka'da ne kadar süreyle saklanacağını topic bazlı bir yapılandırma ayarıyla belirlersiniz ve bu sürenin sonunda eski mesajlar sistemden kaldırılır **(retention)**. Kafka'nın performansı veri boyutundan bağımsız olarak büyük ölçüde sabit kaldığından, verilerin uzun süre saklanmasında herhangi bir sakınca yoktur.

Topicler **partition'lara** ayrılır; yani bir topic, farklı Kafka broker'ları üzerinde yer alan bir dizi bucket'a dağıtılır. Verilerinizin bu şekilde dağıtık olarak konumlandırılması ölçeklenebilirlik açısından büyük önem taşır; zira bu yapı, client uygulamaların verileri aynı anda birçok broker'dan okumasına ve bu broker'lara yazmasına olanak tanır. Bir topic'e yeni bir event gönderildiğinde, bu event aslında o topic'in partition'larından birine eklenir. Aynı event key değerine (örneğin bir müşteri veya araç ID'si) sahip mesajlar aynı partition'a yazılır; Kafka, belirli bir topic-partition ikilisini okuyan herhangi bir consumer'ın, o partiton'daki mesajları her zaman tam olarak yazıldıkları sırayla okumasını garanti eder. 

### Brokers

Bir Kafka cluster'ındaki bir sunucu **broker** olarak adlandırılır. Bir Kafka cluster'ı tipik olarak birkaç broker'dan oluşur. Broker, producer'lardan gelen mesajları alır, bunlara offset atar ve mesajları diskteki depolama alanına kalıcı olarak yazar. Ayrıca consumer'lara da hizmet vererek, partition'lar için gelen veri çekme (fetch) taleplerini karşılar ve diske yazılmış olan mesajlarla yanıt verir. Kullanılan donanıma ve bu donanımın performans özelliklerine bağlı olarak, tek bir broker saniyede binlerce partiton ve milyonlarca mesajı rahatlıkla işleyebilir.