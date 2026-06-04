# Tez izleme sunumu konuşmacı notları

Bu notlar `tez_izleme_sunumu.tex` dosyasındaki slayt akışına göre hazırlanmıştır.
Amaç, jüriye çalışmanın motivasyonunu, literatür boşluğunu, yöntemini ve bulgularını
net bir hikaye halinde aktarmaktır.

## Sunum stratejisi

- Ana mesaj: Bu tez, test odası çizelgelemesinde numune sayısını sabit kabul etmek yerine
  karar değişkeni yaparak iş yükü ve paralellik arasındaki dengeyi optimize eder.
- Vurgu sırası: endüstriyel problem -> araştırma boşluğu -> TCSSOP tanımı -> entegre
  algoritma -> deneysel kanıt -> yönetimsel çıkarımlar.
- Jürinin özellikle sorabileceği noktalar: numune artışının neden gecikmeyi azaltabildiği,
  VNS'in neden numune vektörü üzerinde çalıştığı, sabit numune politikasına göre adil
  karşılaştırmanın nasıl yapıldığı ve fizibilite doğrulamasının kapsamı.

## Slayt bazlı anlatım notları

1. **Başlık**
   - Çalışmanın buzdolabı R&D test süreçlerinde test odası kapasitesinin daha verimli
     kullanılmasına odaklandığını söyleyin.

2. **Sunum akışı**
   - Önce problem ve literatür boşluğu, sonra model ve yöntem, en son deneysel sonuçlar
     ve tez izleme için sonraki teknik geliştirme alanları anlatılacak.

3. **Endüstriyel motivasyon**
   - Test gecikmelerinin doğrudan sertifikasyon, üretim başlangıcı ve pazara çıkış
     tarihlerini etkilediğini vurgulayın.
   - Sabit numune politikasının pratikte basit olduğunu, fakat darboğazlara duyarsız
     kaldığını belirtin.

4. **Test odası sistemi**
   - Aynı odadaki istasyonların ortak çevresel koşulda çalışmasının, oda atamasını
     çizelgeleme kararlarıyla sıkı biçimde bağladığını açıklayın.

5. **Test akışı ve numune etkisi**
   - Çalışmanın temel sezgisi burada: numune artışı pull-down iş yükünü artırır; ancak
     sonraki testlerde aynı ürünün farklı numuneleri paralel yürütülebilir.

6. **Literatürde konumlandırma**
   - Literatürde çizelgeleme ve test planlamanın çoğu zaman ayrı ele alındığını söyleyin.
   - Bu çalışmada iş yükünün kendisi numune kararıyla endojen hale geliyor.

7. **Çalışmanın katkıları**
   - Katkıları üç başlıkta özetleyin: yeni problem yapısı, entegre çözüm yaklaşımı,
     iş yükü odaklı VNS.

8. **TCSSOP karar bileşenleri**
   - Üç kararın birbirinden bağımsız olmadığını vurgulayın: numune vektörü iş yükünü,
     oda ataması kapasite dağılımını, çizelge ise gerçek gecikmeyi belirler.

9. **Amaç fonksiyonu**
   - Birincil amacın toplam gecikme olduğunu, numune sayısının ise eşit gecikmede
     ikincil kriter olarak kullanıldığını belirtin.

10. **Fizibilite koşulları**
    - Endüstriyel uygulanabilirlik için yalnızca düşük gecikme değil, tüm çevresel,
      kaynak ve aşama koşullarının sağlanmasının gerektiğini söyleyin.

11. **Genel çözüm çerçevesi**
    - Algoritmayı geri beslemeli bir yapı olarak anlatın: numune değişir, iş yükü değişir,
      oda ataması tekrar değerlendirilir, çizelge yeniden kurulur.

12. **Oda çevre ataması**
    - Exact modelin amaçının kapasiteyi iş yükü paylarına göre dengelemek olduğunu;
      yerel iyileştirmenin ise çizelgeleme etkilerini daha iyi yakaladığını belirtin.

13. **Numune sayısı optimizasyonu**
    - Tek ürün hareketleri ve late--early rebalance operatörünün farklı rolleri olduğunu
      anlatın: biri yerel ayar, diğeri sistem düzeyinde yeniden dağıtım sağlar.

14. **Çizelgeleme prosedürü**
    - EDD kuralının teslim tarihi baskısını yönetmek için seçildiğini, fakat hazır olma
      koşullarının ürün aşama durumuna ve numune uygunluğuna bağlı olduğunu açıklayın.

15. **VNS çeşitlendirme**
    - VNS'in klasik sıralama hareketlerinden farklı olarak numune vektörünü değiştirdiğini
      söyleyin; bu nedenle algoritma alternatif iş yükü yapıları keşfeder.

16. **Deneysel tasarım**
    - 90 senaryonun rastgele değil, kontrollü faktöriyel tasarımla üretildiğini vurgulayın:
      test matrisi, teslim tarihi senaryosu ve voltaj yoğunluğu.

17. **Karşılaştırılan yöntemler**
    - Ablation yapısının her bileşenin katkısını ayırmaya yaradığını açıklayın.

18. **Parametre ayarları**
    - Parametrelerin örneğe özel ayarlanmadığını söyleyin; bu, sonuçların daha güvenilir
      yorumlanmasını sağlar.

19. **VNS oran politikası**
    - 0.20--0.30 politikasının, çok küçük ve çok büyük pertürbasyonlar arasında daha
      dengeli bir çeşitlendirme sağladığını belirtin.

20. **Shake bütçesi**
    - Kmax=200'ün daha yüksek süre gerektirdiğini; ancak taktik planlama bağlamında daha
      istikrarlı ve kaliteli çözümler verdiğini söyleyin.

21. **Genel benchmark**
    - Ana bulgu: seçilen önerilen yapı tüm 90 senaryoda en iyi sonucu vermiştir.
    - Sabit numune, no-outer ve no-VNS varyantlarının neden eksik kaldığını kısaca bağlayın.

22. **Sabit numune ile doğrudan karşılaştırma**
    - Önerilen yöntemin daha fazla numune kullandığını saklamayın; bunun seçici paralellik
      yaratmak için yapıldığını ve leksikografik amaçta gecikmenin birincil olduğunu belirtin.

23. **Yönetimsel çıkarımlar**
    - Pratik mesaj: kapasite sorununa her ürüne aynı numune sayısını vererek değil, hangi
      ürünlerde ek numunenin gecikmeyi azalttığını hesaplayarak yaklaşmak gerekir.

24. **Mevcut durum ve geliştirilebilecek noktalar**
    - Tezin şu ana kadar tamamlanan modelleme, algoritma ve deneysel çalışma boyutlarını
      özetleyin.
    - Gelecek çalışmaların modeli daha gerçekçi kılacak belirsizlik ve maliyet boyutlarına
      odaklanabileceğini söyleyin.

25. **Sonuç**
    - Mesajı tekrar tek cümleyle kapatın: karar-bağımlı iş yükü olan test sistemlerinde
      numune, oda ve çizelge kararları birlikte optimize edilmelidir.

26. **Teşekkürler**
    - Sorulara geçmeden önce jüriye özellikle yöntem veya deneysel tasarım hakkında
      görüşlerini duymaktan memnun olacağınızı söyleyebilirsiniz.

## Olası jüri soruları için kısa yanıtlar

**Soru:** Numune sayısı artarken iş yükü artıyor; neden gecikme azalıyor?

**Yanıt:** Pull-down iş yükü artıyor, ancak downstream testlerde aynı ürüne ait farklı
numuneler paralel yürütülebiliyor. Bu nedenle etki monoton değil; belirli ürünlerde ek
numune tamamlanma zamanını düşürebiliyor.

**Soru:** VNS neden oda ataması veya sıralama üzerinde değil de numune üzerinde çalışıyor?

**Yanıt:** Bu problemde numune vektörü iş yükünün yapısını belirliyor. Numuneyi pertürbe
etmek, yalnızca sıralamayı değiştirmekten daha farklı bölgeleri keşfetmeyi sağlıyor.

**Soru:** Önerilen yöntem daha fazla numune kullanıyor; bu dezavantaj mı?

**Yanıt:** Amaç leksikografik: önce gecikme, eşitlikte numune sayısı. Ek numune rastgele
değil, gecikmeyi azaltan ürünlere seçici olarak atanıyor. Eğer numune maliyeti birincil
hale getirilmek istenirse model çok amaçlı biçimde genişletilebilir.

**Soru:** Sonuçlar gerçek veri mi?

**Yanıt:** Ham veri gizliliği nedeniyle senaryolar sentetik üretilmiştir; ancak parametre
aralıkları ve yapısal kurallar endüstriyel test pratiğine göre kalibre edilmiştir.

**Soru:** Çizelgelerin uygulanabilir olduğundan nasıl emin olunuyor?

**Yanıt:** Çizelge sonrasında gerekli test tutarlılığı, çevre uyumu, nem ve voltaj
uygunluğu, istasyon çakışmaları, numune çakışmaları ve aşama mantığı ayrı ayrı doğrulanıyor.
